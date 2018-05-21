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
package io.druid.server.router.setup;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.router.interpolator.QueryInterpolator;
import org.codehaus.jackson.annotate.JsonCreator;

import java.util.List;

public class QueryProxyBehaviorConfig
{
  public static String CONFIG_KEY = "router.config";
  private final List<QueryInterpolator> queryInterpolators;
  private final int queryQueueSize;
  private final int startDelayMillis;
  private final int maxRunningQueries;

  private static final int DEFAULT_QUERY_QUEUE_SIZE = 1000;
  private static final int DEFAULT_START_DELAY_MILLIS = 1000;
  private static final int DEFAULT_MAX_RUNNING_QUERIES = 20;

  private static final Logger logger = new Logger(QueryProxyBehaviorConfig.class);


  public QueryProxyBehaviorConfig()
  {
    queryInterpolators = ImmutableList.of();
    queryQueueSize = DEFAULT_QUERY_QUEUE_SIZE;
    startDelayMillis = DEFAULT_START_DELAY_MILLIS;
    maxRunningQueries = DEFAULT_MAX_RUNNING_QUERIES;
  }

  @JsonCreator
  public QueryProxyBehaviorConfig(
      @JsonProperty("interpolators") List<QueryInterpolator> queryInterpolators,
      @JsonProperty("queryQueueSize") int queryQueueSize,
      @JsonProperty("startDelayMillis") int startDelayMillis,
      @JsonProperty("maxRunningQueries") int maxRunningQueries
  )
  {
    this.queryInterpolators = queryInterpolators;
    this.queryQueueSize = queryQueueSize <= 0 ? DEFAULT_QUERY_QUEUE_SIZE : queryQueueSize;
    this.startDelayMillis = startDelayMillis <= 0 ? DEFAULT_START_DELAY_MILLIS : startDelayMillis;
    this.maxRunningQueries = maxRunningQueries <= 0 ? DEFAULT_MAX_RUNNING_QUERIES : maxRunningQueries;

    logger.info(StringUtils.format(
        "queryQueueSize=%d\nstartDelayMillis=%d\nmaxRunningQueries=%d",
        this.queryQueueSize,
        this.startDelayMillis,
        this.maxRunningQueries
    ));
  }

  @JsonProperty("interpolators")
  public List<QueryInterpolator> getQueryInterpolators()
  {
    return this.queryInterpolators;
  }

  @JsonProperty("queryQueueSize")
  public int getQueryQueueSize()
  {
    return queryQueueSize;
  }

  @JsonProperty("startDelayMillis")
  public int getStartDelayMillis()
  {
    return startDelayMillis;
  }

  @JsonProperty("maxRunningQueries")
  public int getMaxRunningQueries()
  {
    return maxRunningQueries;
  }

}
