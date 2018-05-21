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
package io.druid.server.router;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.EntryExistsException;
import io.druid.query.Query;
import io.druid.server.router.setup.QueryProxyBehaviorConfig;
import io.druid.server.AsyncQueryForwardingServlet;
import org.eclipse.jetty.client.api.Request;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class QueryQueue
{
  private final Supplier<QueryProxyBehaviorConfig> proxyConfigRef;
  private final LinkedList<Query> queries = Lists.newLinkedList();
  private final Map<String, QueryStuff> queryMap = Maps.newHashMap();

  private final ReentrantLock lock;
  private final Condition notEmpty;

  private static final Logger logger = new Logger(QueryQueue.class);


  @Inject
  public QueryQueue(
      final Supplier<QueryProxyBehaviorConfig> proxyConfigRef
  )
  {
    this.proxyConfigRef = proxyConfigRef;
    lock = new ReentrantLock(true);
    notEmpty = lock.newCondition();
  }

  public QueryStuff getQueryById(String id)
  {
    return queryMap.get(id);
  }

  public List<QueryStuff> getQueries()
  {
    return ImmutableList.copyOf(queryMap.values());
  }

  public boolean add(final Query query, HttpServletRequest request, HttpServletResponse response, AsyncQueryForwardingServlet servlet)
      throws EntryExistsException, ServletException, IOException
  {
    lock.lock();
    try {
      Preconditions.checkNotNull(query, "query");
      Preconditions.checkNotNull(request, "request");
      Preconditions.checkNotNull(response, "response");
      if (queries.size() > proxyConfigRef.get().getQueryQueueSize()) {
        return false;
      }
      if (queryMap.containsKey(query.getId())) {
        throw new EntryExistsException(query.getId());
      }
      logger.info("Adding query %s", query.getId());
      queryMap.put(
          query.getId(),
          new QueryStuff(
              query,
              DateTimes.nowUtc(),
              request,
              response
          )
      );
      queries.add(query);
      logger.debug("Starting async on request");

      Request proxyRequest = servlet.prepareService(request, response);
      queryMap.get(query.getId()).setProxyRequest(proxyRequest);

      notEmpty.signal();
      return true;
    }
    finally {
      lock.unlock();
    }
  }

  public QueryStuff pollNextToRun() throws InterruptedException
  {
    lock.lockInterruptibly();
    try {
      while (queries.isEmpty()) {
        notEmpty.await();
      }
      Query q = queries.remove();
      return queryMap.remove(q.getId());
    }
    finally {
      lock.unlock();
    }
  }

  public static class QueryStuff
  {
    final Query query;
    final DateTime createdDate;
    final HttpServletRequest request;
    final HttpServletResponse response;
    Request proxyRequest;

    private QueryStuff(
        Query query,
        DateTime createdDate,
        HttpServletRequest request,
        HttpServletResponse response
    )
    {
      this.query = query;
      this.createdDate = createdDate;
      this.request = request;
      this.response = response;
    }

    public void setProxyRequest(Request request) 
    {
      this.proxyRequest = request;
    }

    @JsonProperty("query")
    public Query getQuery()
    {
      return query;
    }

    @JsonProperty("createdDate")
    public DateTime getCreatedDate()
    {
      return createdDate;
    }

    public HttpServletRequest getRequest()
    {
      return request;
    }

    public HttpServletResponse getResponse()
    {
      return response;
    }

    public Request getProxyRequest() 
    {
      return proxyRequest;
    }
  }
}
