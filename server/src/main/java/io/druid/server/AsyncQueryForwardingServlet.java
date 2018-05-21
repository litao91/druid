/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.druid.client.selector.Server;
import io.druid.common.guava.DSuppliers;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.jackson.JacksonUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.metadata.EntryExistsException;
import io.druid.query.DruidMetrics;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.server.log.RequestLogger;
import io.druid.server.metrics.QueryCountStatsProvider;
import io.druid.server.router.QueryHostFinder;
import io.druid.server.router.QueryQueue;
import io.druid.server.router.Router;
import io.druid.server.router.interpolator.QueryInterpolator;
import io.druid.server.router.setup.QueryProxyBehaviorConfig;
import io.druid.server.security.AuthConfig;
import org.apache.http.client.utils.URIBuilder;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.proxy.AsyncProxyServlet;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.server.HttpOutput;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.client.util.DeferredContentProvider;
import org.joda.time.DateTime;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.AsyncContext;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;


import java.io.Closeable;
import java.util.Iterator;

import org.eclipse.jetty.client.AsyncContentProvider;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.util.IteratingCallback;


/**
 * This class does async query processing and should be merged with QueryResource at some point
 */
public class AsyncQueryForwardingServlet extends AsyncProxyServlet implements QueryCountStatsProvider
{
  private static final String CONTINUE_ACTION_ATTRIBUTE = ProxyServlet.class.getName() + ".continueAction";
  private static final EmittingLogger log = new EmittingLogger(AsyncQueryForwardingServlet.class);
  private static final Logger logger = new Logger(AsyncQueryForwardingServlet.class);
  @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
  private static final String APPLICATION_SMILE = "application/smile";

  private static final String HOST_ATTRIBUTE = "io.druid.proxy.to.host";
  private static final String SCHEME_ATTRIBUTE = "io.druid.proxy.to.host.scheme";
  private static final String QUERY_ATTRIBUTE = "io.druid.proxy.query";
  private static final String AVATICA_QUERY_ATTRIBUTE = "io.druid.proxy.avaticaQuery";
  private static final String OBJECTMAPPER_ATTRIBUTE = "io.druid.proxy.objectMapper";

  private static final int CANCELLATION_TIMEOUT_MILLIS = 500;

  private final AtomicLong successfulQueryCount = new AtomicLong();
  private final AtomicLong failedQueryCount = new AtomicLong();
  private final AtomicLong interruptedQueryCount = new AtomicLong();

  private static void handleException(HttpServletResponse response, ObjectMapper objectMapper, Exception exception)
      throws IOException
  {
    if (!response.isCommitted()) {
      final String errorMessage = exception.getMessage() == null ? "null exception" : exception.getMessage();

      response.resetBuffer();
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      objectMapper.writeValue(
          response.getOutputStream(),
          ImmutableMap.of("error", errorMessage)
      );
    }
    response.flushBuffer();
  }

  private final QueryToolChestWarehouse warehouse;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QueryHostFinder hostFinder;
  private final Provider<HttpClient> httpClientProvider;
  private final DruidHttpClientConfig httpClientConfig;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final GenericQueryMetricsFactory queryMetricsFactory;
  private final Supplier<QueryProxyBehaviorConfig> proxyBehaviorConfigRef;
  private final QueryQueue queryQueue;
  // from query id to timeout
  private final Map<String, DateTime> runningQueries = Maps.newHashMap();
  private final ReentrantLock runningQueryLock;
  private final Condition notFull;


  private final ExecutorService managerExec = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
          .setDaemon(false)
          .setNameFormat("QueryQueue-Manager").build()
  );

  private HttpClient broadcastClient;

  /**
   * This is strictly for testing purpose only, never use this contructor
   */
  public AsyncQueryForwardingServlet(
      QueryToolChestWarehouse warehouse,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      @Router Provider<HttpClient> httpClientProvider,
      @Router DruidHttpClientConfig httpClientConfig,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      GenericQueryMetricsFactory queryMetricsFactory
  )
  {
    this.warehouse = warehouse;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.httpClientProvider = httpClientProvider;
    this.httpClientConfig = httpClientConfig;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.queryMetricsFactory = queryMetricsFactory;
    this.proxyBehaviorConfigRef = DSuppliers.of(new AtomicReference<>(new QueryProxyBehaviorConfig()));
    this.queryQueue = new QueryQueue(this.proxyBehaviorConfigRef);
    runningQueryLock = new ReentrantLock();
    notFull = runningQueryLock.newCondition();
  }

  @Inject
  public AsyncQueryForwardingServlet(
      QueryToolChestWarehouse warehouse,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      @Router Provider<HttpClient> httpClientProvider,
      @Router DruidHttpClientConfig httpClientConfig,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      GenericQueryMetricsFactory queryMetricsFactory,
      final Supplier<QueryProxyBehaviorConfig> proxyConfigRef
  )
  {
    this.warehouse = warehouse;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.httpClientProvider = httpClientProvider;
    this.httpClientConfig = httpClientConfig;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.queryMetricsFactory = queryMetricsFactory;
    this.proxyBehaviorConfigRef = proxyConfigRef;
    this.queryQueue = new QueryQueue(this.proxyBehaviorConfigRef);
    runningQueryLock = new ReentrantLock();
    notFull = runningQueryLock.newCondition();
  }

  @Override
  public void init() throws ServletException
  {
    super.init();

    // Note that httpClientProvider is setup to return same HttpClient instance on each get() so
    // it is same http client as that is used by parent ProxyServlet.
    broadcastClient = newHttpClient();
    try {
      broadcastClient.start();
    }
    catch (Exception e) {
      throw new ServletException(e);
    }

    managerExec.submit(
        () -> {
          while (true) {
            try {
              manageQueries();
            }
            catch (InterruptedException e) {
              logger.info("Interrupted exiting!");
              break;
            }
            catch (Exception e) {
              log.makeAlert(e, "Failed to manage");
              try {
                Thread.sleep(this.proxyBehaviorConfigRef.get().getStartDelayMillis());
              }
              catch (InterruptedException e2) {
                logger.info("Interrupted exiting");
                break;
              }
            }
          }
        });
  }

  private void manageQueries() throws InterruptedException
  {
    logger.info("Beginning management in %s.", proxyBehaviorConfigRef.get().getStartDelayMillis());
    while (true) {
      final QueryQueue.QueryStuff queryStuff = queryQueue.pollNextToRun();
      logger.debug("polled next query: " + queryStuff.getQuery().getId());

      runningQueryLock.lock();
      try {
        while (runningQueries.size() >= proxyBehaviorConfigRef.get().getMaxRunningQueries()) {
          logger.info(StringUtils.format(
                "runningQuerySize(%d) >= maxRunningQueries(%d), wait for query to complete",
                runningQueries.size(), proxyBehaviorConfigRef.get().getMaxRunningQueries()
                ));

          notFull.await();
        }
        DateTime now = DateTimes.nowUtc();
        long timeout = 300000;
        if (queryStuff.getQuery().getContext().containsKey("timeout")) {
          timeout = (Long) queryStuff.getQuery().getContext().get("timeout");
        }
        logger.debug("Asking query %s to run", queryStuff.getQuery().getId());
        runningQueries.put(queryStuff.getQuery().getId(), now.plus(timeout));
        sendProxyRequest(queryStuff.getRequest(), queryStuff.getResponse(), queryStuff.getProxyRequest());
      }
      catch (Exception e) {
        logger.error(e, "do Service error for query %s", queryStuff.getQuery().getId());
        runningQueries.remove(queryStuff.getQuery().getId());
      }
      finally {
        runningQueryLock.unlock();
      }
    }
  }

  @Override
  public void destroy()
  {
    super.destroy();
    try {
      broadcastClient.stop();
    }
    catch (Exception e) {
      logger.warn(e, "Error stopping servlet");
    }
  }

  public Request prepareService(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
  {
    final int requestId = getRequestId(request);

    String rewrittenTarget = rewriteTarget(request);

    if (_log.isDebugEnabled()) {
      StringBuffer uri = request.getRequestURL();
      if (request.getQueryString() != null) {
        uri.append("?").append(request.getQueryString());
      }
      if (_log.isDebugEnabled()) {
        _log.debug("{} rewriting: {} -> {}", requestId, uri, rewrittenTarget);
      }
    }

    if (rewrittenTarget == null) {
      onProxyRewriteFailed(request, response);
      return null;
    }

    final Request proxyRequest = getHttpClient().newRequest(rewrittenTarget)
        .method(request.getMethod())
        .version(HttpVersion.fromString(request.getProtocol()));

    copyRequestHeaders(request, proxyRequest);

    addProxyHeaders(request, proxyRequest);

    final AsyncContext asyncContext = request.startAsync();
    // We do not timeout the continuation, but the proxy request
    asyncContext.setTimeout(0);
    proxyRequest.timeout(getTimeout(), TimeUnit.MILLISECONDS);

    if (hasContent(request)) {
      if (expects100Continue(request)) {
        DeferredContentProvider deferred = new DeferredContentProvider();
        proxyRequest.content(deferred);
        proxyRequest.attribute(CLIENT_REQUEST_ATTRIBUTE, request);
        proxyRequest.attribute(CONTINUE_ACTION_ATTRIBUTE, (Runnable) () -> {
          try {
            ContentProvider provider = proxyRequestContent(request, response, proxyRequest);
            new DelegatingContentProvider(request, proxyRequest, response, provider, deferred).iterate();
          }
          catch (Throwable failure) {
            onClientRequestFailure(request, proxyRequest, response, failure);
          }
        });
      } else {
        proxyRequest.content(proxyRequestContent(request, response, proxyRequest));
      }
    }

    // Since we can't see the request object on the remote side, we can't check whether the remote side actually
    // performed an authorization check here, so always set this to true for the proxy servlet.
    // If the remote node failed to perform an authorization check, PreResponseAuthorizationCheckFilter
    // will log that on the remote node.
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);

    return proxyRequest;
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
  {
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(request.getContentType())
                            || APPLICATION_SMILE.equals(request.getContentType());
    final ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;
    request.setAttribute(OBJECTMAPPER_ATTRIBUTE, objectMapper);

    final String requestURI = request.getRequestURI();
    final String method = request.getMethod();
    final Server targetServer;

    // The Router does not have the ability to look inside SQL queries and route them intelligently, so just treat
    // them as a generic request.
    final boolean isQueryEndpoint = requestURI.startsWith("/druid/v2")
                                    && !requestURI.startsWith("/druid/v2/sql");

    final boolean isAvatica = requestURI.startsWith("/druid/v2/sql/avatica");

    if (isAvatica) {
      Map<String, Object> requestMap = objectMapper.readValue(
          request.getInputStream(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
      String connectionId = getAvaticaConnectionId(requestMap);
      targetServer = hostFinder.findServerAvatica(connectionId);
      byte[] requestBytes = objectMapper.writeValueAsBytes(requestMap);
      request.setAttribute(AVATICA_QUERY_ATTRIBUTE, requestBytes);
    } else if (isQueryEndpoint && HttpMethod.DELETE.is(method)) {
      // query cancellation request
      targetServer = hostFinder.pickDefaultServer();

      for (final Server server : hostFinder.getAllServers()) {
        // send query cancellation to all brokers this query may have gone to
        // to keep the code simple, the proxy servlet will also send a request to the default targetServer.
        if (!server.getHost().equals(targetServer.getHost())) {
          // issue async requests
          Response.CompleteListener completeListener = result -> {
            if (result.isFailed()) {
              logger.warn(
                  result.getFailure(),
                  "Failed to forward cancellation request to [%s]",
                  server.getHost()
              );
            }
          };

          Request broadcastReq = broadcastClient
              .newRequest(rewriteURI(request, server.getScheme(), server.getHost()))
              .method(HttpMethod.DELETE)
              .timeout(CANCELLATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

          copyRequestHeaders(request, broadcastReq);
          broadcastReq.send(completeListener);
        }
        interruptedQueryCount.incrementAndGet();
      }
    } else if (isQueryEndpoint && HttpMethod.POST.is(method)) {
      // query request
      try {
        Query inputQuery = objectMapper.readValue(request.getInputStream(), Query.class);
        if (inputQuery.getId() == null) {
          inputQuery = inputQuery.withId(UUID.randomUUID().toString());
        }
        if (inputQuery != null) {
          // check the interpolators
          for (QueryInterpolator interpolator : proxyBehaviorConfigRef.get().getQueryInterpolators()) {
            QueryInterpolator.InterpolateResult r = interpolator.runInterpolation(inputQuery, this);
            if (!r.queryShouldRun()) {
              logger.warn("Query banned by interpolator: " + interpolator.toString());
              final String errorMessage = r.getMessage();
              requestLogger.log(
                  new RequestLogLine(
                      DateTimes.nowUtc(),
                      request.getRemoteAddr(),
                      inputQuery,
                      new QueryStats(ImmutableMap.of("success", false, "exception", errorMessage))
                  )
              );
              response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
              response.setContentType(MediaType.APPLICATION_JSON);
              objectMapper.writeValue(
                  response.getOutputStream(),
                  ImmutableMap.of("error", errorMessage)
              );
              return;
            }
          }          // find the appropriate server
          targetServer = hostFinder.pickServer(inputQuery);
        } else {
          targetServer = hostFinder.pickDefaultServer();
        }
        request.setAttribute(QUERY_ATTRIBUTE, inputQuery);
      }
      catch (IOException e) {
        logger.warn(e, "Exception parsing query");
        final String errorMessage = e.getMessage() == null ? "no error message" : e.getMessage();
        requestLogger.log(
            new RequestLogLine(
                DateTimes.nowUtc(),
                request.getRemoteAddr(),
                null,
                new QueryStats(ImmutableMap.<String, Object>of("success", false, "exception", errorMessage))
            )
        );
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        response.setContentType(MediaType.APPLICATION_JSON);
        objectMapper.writeValue(
            response.getOutputStream(),
            ImmutableMap.of("error", errorMessage)
        );

        return;
      }
      catch (Exception e) {
        handleException(response, objectMapper, e);
        return;
      }
    } else {
      targetServer = hostFinder.pickDefaultServer();
    }

    request.setAttribute(HOST_ATTRIBUTE, targetServer.getHost());
    request.setAttribute(SCHEME_ATTRIBUTE, targetServer.getScheme());

    if (isQueryEndpoint && HttpMethod.POST.is(method)) {
      try {
        Query query = (Query) request.getAttribute(QUERY_ATTRIBUTE);
        logger.info("Adding query: %s to query queue, request: %s, response: %s", query.getId(), request.toString(), response.toString());
        if (!queryQueue.add(query, request, response, this)) {
          response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          response.setContentType(MediaType.APPLICATION_JSON);
          objectMapper.writeValue(
              response.getOutputStream(),
              ImmutableMap.of("error", "Query queue full, please try again later")
          );
        }
      }
      catch (EntryExistsException e) {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        response.setContentType(MediaType.APPLICATION_JSON);
        final String errorMessage = e.getMessage() == null ? "no error message" : e.getMessage();
        objectMapper.writeValue(
            response.getOutputStream(),
            ImmutableMap.of("error", errorMessage)
        );
      }
    } else {
      // run service directly
      doService(request, response);
    }
  }

  protected void doService(
      HttpServletRequest request,
      HttpServletResponse response
  ) throws ServletException, IOException
  {
    // Just call the superclass service method. Overriden in tests.
    super.service(request, response);
  }

  @Override
  protected void sendProxyRequest(
      HttpServletRequest clientRequest,
      HttpServletResponse proxyResponse,
      Request proxyRequest
  )
  {
    proxyRequest.timeout(httpClientConfig.getReadTimeout().getMillis(), TimeUnit.MILLISECONDS);
    proxyRequest.idleTimeout(httpClientConfig.getReadTimeout().getMillis(), TimeUnit.MILLISECONDS);

    byte[] avaticaQuery = (byte[]) clientRequest.getAttribute(AVATICA_QUERY_ATTRIBUTE);
    if (avaticaQuery != null) {
      proxyRequest.content(new BytesContentProvider(avaticaQuery));
    }

    final Query query = (Query) clientRequest.getAttribute(QUERY_ATTRIBUTE);
    if (query != null) {
      final ObjectMapper objectMapper = (ObjectMapper) clientRequest.getAttribute(OBJECTMAPPER_ATTRIBUTE);
      try {
        proxyRequest.content(new BytesContentProvider(objectMapper.writeValueAsBytes(query)));
      }
      catch (JsonProcessingException e) {
        Throwables.propagate(e);
      }
    }

    super.sendProxyRequest(
        clientRequest,
        proxyResponse,
        proxyRequest
    );
  }

  @Override
  protected Response.Listener newProxyResponseListener(
      HttpServletRequest request, HttpServletResponse response
  )
  {
    final Query query = (Query) request.getAttribute(QUERY_ATTRIBUTE);
    if (query != null) {
      return newQueryForwardingProxyResponseListener(request, response, query, System.nanoTime());
    } else {
      return super.newProxyResponseListener(request, response);
    }
  }

  @Override
  protected String rewriteTarget(HttpServletRequest request)
  {
    return rewriteURI(
        request,
        (String) request.getAttribute(SCHEME_ATTRIBUTE),
        (String) request.getAttribute(HOST_ATTRIBUTE)
    ).toString();
  }

  protected URI rewriteURI(HttpServletRequest request, String scheme, String host)
  {
    return makeURI(scheme, host, request.getRequestURI(), request.getQueryString());
  }

  protected static URI makeURI(String scheme, String host, String requestURI, String rawQueryString)
  {
    try {
      return new URIBuilder()
          .setScheme(scheme)
          .setHost(host)
          .setPath(requestURI)
          // No need to encode-decode queryString, it is already encoded
          .setQuery(rawQueryString)
          .build();
    }
    catch (URISyntaxException e) {
      logger.error(e, "Unable to rewrite URI [%s]", e.getMessage());
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected HttpClient newHttpClient()
  {
    return httpClientProvider.get();
  }

  @Override
  protected HttpClient createHttpClient() throws ServletException
  {
    HttpClient client = super.createHttpClient();
    // override timeout set in ProxyServlet.createHttpClient
    setTimeout(httpClientConfig.getReadTimeout().getMillis());
    return client;
  }

  private Response.Listener newQueryForwardingProxyResponseListener(
      HttpServletRequest request,
      HttpServletResponse response,
      Query query,
      long startNs
  )
  {
    return new QueryForwardingProxyResponseListener(request, response, query, startNs);
  }

  @Override
  public long getSuccessfulQueryCount()
  {
    return successfulQueryCount.get();
  }

  @Override
  public long getFailedQueryCount()
  {
    return failedQueryCount.get();
  }

  @Override
  public long getInterruptedQueryCount()
  {
    return interruptedQueryCount.get();
  }

  private static String getAvaticaConnectionId(Map<String, Object> requestMap) throws IOException
  {
    Object connectionIdObj = requestMap.get("connectionId");
    if (connectionIdObj == null) {
      throw new IAE("Received an Avatica request without a connectionId.");
    }
    if (!(connectionIdObj instanceof String)) {
      throw new IAE("Received an Avatica request with a non-String connectionId.");
    }

    return (String) connectionIdObj;
  }

  private class QueryForwardingProxyResponseListener extends ProxyResponseListener
  {
    private final HttpServletRequest req;
    private final HttpServletResponse res;
    private final Query query;
    private final long startNs;

    public QueryForwardingProxyResponseListener(
        HttpServletRequest request,
        HttpServletResponse response,
        Query query,
        long startNs
    )
    {
      super(request, response);

      this.req = request;
      this.res = response;
      this.query = query;
      this.startNs = startNs;
    }

    @Override
    public void onContent(final Response proxyResponse, ByteBuffer content, final Callback callback)
    {
      logger.info("Query %s content", query.getId());
      runningQueryLock.lock();
      try {
        runningQueries.remove(query.getId());
        notFull.signal();
      }
      finally {
        runningQueryLock.unlock();
      }

      try {
        ((HttpOutput) this.res.getOutputStream()).reopen();
      }
      catch (Exception e) {
        logger.error(e, "error");
      }
      logger.info("Query %s removed from running queries", query.getId());
      super.onContent(proxyResponse, content, callback);
    }

    @Override
    public void onComplete(Result result)
    {

      final long requestTimeNs = System.nanoTime() - startNs;
      try {
        boolean success = result.isSucceeded();
        if (success) {
          successfulQueryCount.incrementAndGet();
        } else {
          failedQueryCount.incrementAndGet();
        }
        emitQueryTime(requestTimeNs, success);
        requestLogger.log(
            new RequestLogLine(
                DateTimes.nowUtc(),
                req.getRemoteAddr(),
                query,
                new QueryStats(
                    ImmutableMap.of(
                        "query/time",
                        TimeUnit.NANOSECONDS.toMillis(requestTimeNs),
                        "success",
                        success
                        && result.getResponse().getStatus() == javax.ws.rs.core.Response.Status.OK.getStatusCode()
                    )
                )
            )
        );


      }
      catch (Exception e) {
        logger.error(e, "Unable to log query [%s]!", query);
      }

      super.onComplete(result);
    }

    @Override
    public void onFailure(Response response, Throwable failure)
    {
      try {
        final String errorMessage = failure.getMessage();
        failedQueryCount.incrementAndGet();
        emitQueryTime(System.nanoTime() - startNs, false);
        requestLogger.log(
            new RequestLogLine(
                DateTimes.nowUtc(),
                req.getRemoteAddr(),
                query,
                new QueryStats(
                    ImmutableMap.<String, Object>of(
                        "success",
                        false,
                        "exception",
                        errorMessage == null ? "no message" : errorMessage
                    )
                )
            )
        );
      }
      catch (IOException logError) {
        logger.error(logError, "Unable to log query [%s]!", query);
      }

      log.makeAlert(failure, "Exception handling request")
         .addData("exception", failure.toString())
         .addData("query", query)
         .addData("peer", req.getRemoteAddr())
         .emit();

      super.onFailure(response, failure);
    }

    private void emitQueryTime(long requestTimeNs, boolean success) throws JsonProcessingException
    {
      QueryMetrics queryMetrics = DruidMetrics.makeRequestMetrics(
          queryMetricsFactory,
          warehouse.getToolChest(query),
          query,
          req.getRemoteAddr()
      );
      queryMetrics.success(success);
      queryMetrics.reportQueryTime(requestTimeNs).emit(emitter);
    }
  }

  private class DelegatingContentProvider extends IteratingCallback implements AsyncContentProvider.Listener
  {
    private final HttpServletRequest clientRequest;
    private final Request proxyRequest;
    private final HttpServletResponse proxyResponse;
    private final Iterator<ByteBuffer> iterator;
    private final DeferredContentProvider deferred;

    private DelegatingContentProvider(HttpServletRequest clientRequest, Request proxyRequest, HttpServletResponse proxyResponse, ContentProvider provider, DeferredContentProvider deferred)
    {
      this.clientRequest = clientRequest;
      this.proxyRequest = proxyRequest;
      this.proxyResponse = proxyResponse;
      this.iterator = provider.iterator();
      this.deferred = deferred;
      if (provider instanceof AsyncContentProvider) {
        ((AsyncContentProvider) provider).setListener(this);
      }
    }

    @Override
    protected Action process() throws Exception
    {
      if (!iterator.hasNext()) {
        return Action.SUCCEEDED;
      }

      ByteBuffer buffer = iterator.next();
      if (buffer == null) {
        return Action.IDLE;
      }

      deferred.offer(buffer, this);
      return Action.SCHEDULED;
    }

    @Override
    public void succeeded()
    {
      if (iterator instanceof Callback) {
        ((Callback) iterator).succeeded();
      }
      super.succeeded();
    }

    @Override
    protected void onCompleteSuccess()
    {
      try {
        if (iterator instanceof Closeable) {
          ((Closeable) iterator).close();
        }
        deferred.close();
      }
      catch (Throwable x) {
        _log.ignore(x);
      }
    }

    @Override
    protected void onCompleteFailure(Throwable failure)
    {
      if (iterator instanceof Callback) {
        ((Callback) iterator).failed(failure);
      }
      onClientRequestFailure(clientRequest, proxyRequest, proxyResponse, failure);
    }

    @Override
    public boolean isNonBlocking()
    {
      return true;
    }

    @Override
    public void onContent()
    {
      iterate();
    }
  }
}
