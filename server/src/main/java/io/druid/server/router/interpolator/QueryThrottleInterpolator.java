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

package io.druid.server.router.interpolator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.server.AsyncQueryForwardingServlet;
import io.druid.server.router.QueryQueue;

import java.util.List;

public class QueryThrottleInterpolator extends BlackWhiteListQueryInterpolator
{
  private final int maxRunningQueryNum;
  private final int maxPendingQueryNum;

  private static final Logger logger = new Logger(QueryThrottleInterpolator.class);

  @JsonCreator
  public QueryThrottleInterpolator(
      @JsonProperty("whitelist") List<String> whitelist,
      @JsonProperty("blacklist") List<String> blacklist,
      @JsonProperty("maxRunningQueryNum") int maxRunningQueryNum,
      @JsonProperty("maxPendingQueryNum") int maxPendingQueryNum
  )
  {
    super(whitelist, blacklist);
    this.maxPendingQueryNum = maxPendingQueryNum;
    this.maxRunningQueryNum = maxRunningQueryNum;
  }

  @JsonProperty("maxRunningQueryNum")
  public int getMaxRunningQueryNum()
  {
    return maxRunningQueryNum;
  }

  @JsonProperty("maxPendingQueryNum")
  public int getMaxPendingQueryNum()
  {
    return maxPendingQueryNum;
  }

  @Override
  public InterpolateResult runInterpolation(
      Query query, AsyncQueryForwardingServlet servlet
  )
  {
    if (!shouldApply(query)) {
      return QueryInterpolator.INTERPOLATE_RESULT_OK;
    }

    List<QueryQueue.QueryStuff> running = servlet.getRunningQueries();
    int runningCount = 0;
    for (QueryQueue.QueryStuff q : running) {
      for (String n : query.getDataSource().getNames()) {
        if (q.getQuery().getDataSource().getNames().contains(n)) {
          runningCount++;
          continue;
        }
      }
    }

    if (runningCount > maxRunningQueryNum) {
      return new QueryInterpolator.InterpolateResult(
          false, false,
          StringUtils.format(
              "Currently running queries for datasource %s (%d) > maxAllowed(%d)",
              query.getDataSource().getNames().get(0),
              runningCount,
              maxRunningQueryNum
          )
      );
    }

    logger.info(
          StringUtils.format(
              "Currently running queries for datasource %s (%d) < maxAllowed(%d), fine",
              query.getDataSource().getNames().get(0),
              runningCount,
              maxRunningQueryNum
          ));

    int pendingCount = 0;
    List<QueryQueue.QueryStuff> pending = servlet.getRunningQueries();
    for (QueryQueue.QueryStuff q : pending) {
      for (String n : query.getDataSource().getNames()) {
        if (q.getQuery().getDataSource().getNames().contains(n)) {
          pendingCount++;
          continue;
        }
      }
    }
    if (pendingCount > maxPendingQueryNum) {
      return new QueryInterpolator.InterpolateResult(
          false, false,
          StringUtils.format(
              "Pending queries for datasource %s (%d) > maxAllowed (%d)",
              query.getDataSource().getNames().get(0),
              pendingCount,
              maxPendingQueryNum
          ));
    }
    logger.info(StringUtils.format(
              "Pending queries for datasource %s (%d) < maxAllowed (%d), query allowed",
              query.getDataSource().getNames().get(0),
              pendingCount,
              maxPendingQueryNum
          ));
    return QueryInterpolator.INTERPOLATE_RESULT_OK;
  }

  @Override
  public boolean equals(Object other)
  {
    if (!super.equals(other)) {
      return false;
    }
    if (!(other instanceof QueryThrottleInterpolator)) {
      return false;
    }
    QueryThrottleInterpolator that = (QueryThrottleInterpolator) other;
    if (this.maxPendingQueryNum != that.maxPendingQueryNum) {
      return false;
    }
    if (this.maxRunningQueryNum != that.maxRunningQueryNum) {
      return false;
    }

    return true;
  }
}
