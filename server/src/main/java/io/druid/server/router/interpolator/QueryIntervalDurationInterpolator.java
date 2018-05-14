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
import com.google.common.annotations.VisibleForTesting;
import io.druid.java.util.common.StringUtils;
import io.druid.query.Query;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;

public class QueryIntervalDurationInterpolator extends BlackWhiteListQueryInterpolator
{
  @VisibleForTesting
  private final Duration maxDuration;
  @JsonCreator
  public QueryIntervalDurationInterpolator(
      @JsonProperty("whitelist") List<String> whitelist,
      @JsonProperty("blacklist") List<String> blacklist,
      @JsonProperty("maxDurationMillis") long durationMillis
  )
  {
    super(whitelist, blacklist);
    this.maxDuration = Duration.millis(durationMillis);
  }

  @JsonProperty("maxDurationMillis")
  public long getMaxDuration()
  {
    return maxDuration.getMillis();
  }

  @Override
  public InterpolateResult runInterpolation(Query query)
  {
    if (!shouldApply(query)) {
      return QueryInterpolator.INTERPOLATE_RESULT_OK;
    }

    List<Interval> intervals = query.getIntervals();
    Duration totalDuration = Duration.millis(0);
    for (Interval i: intervals) {
      totalDuration.plus(i.toDuration());
    }
    if (totalDuration.isLongerThan(this.maxDuration)) {
      return new QueryInterpolator.InterpolateResult(false, false,
          StringUtils.format("Duration of the query %d hours > max duration allowed %d hours",
              totalDuration.getStandardHours(),
              maxDuration.getStandardHours()));
    } else {
      return QueryInterpolator.INTERPOLATE_RESULT_OK;
    }
  }

  @Override
  public boolean equals(Object other)
  {
    if (!super.equals(other)) {
      return false;
    }
    if (!(other instanceof QueryIntervalDurationInterpolator)) {
      return false;
    }
    return this.getMaxDuration() == ((QueryIntervalDurationInterpolator) other).getMaxDuration();
  }
}
