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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.EntryExistsException;
import io.druid.query.Query;
import io.druid.server.router.setup.QueryProxyBehaviorConfig;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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

  public boolean add(final Query query, HttpServletRequest request, HttpServletResponse response)
      throws EntryExistsException
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
      // Hack the statemachine of jetty. The request's state must be in async for the
      // Jetty to wait for the response
      request.startAsync();

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

    public Query getQuery()
    {
      return query;
    }

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
  }
}
