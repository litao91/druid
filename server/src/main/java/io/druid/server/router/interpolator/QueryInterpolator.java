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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.query.Query;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "duration", value = QueryIntervalDurationInterpolator.class)
})
public interface QueryInterpolator
{
  InterpolateResult INTERPOLATE_RESULT_OK = new InterpolateResult(false, false, null);
  InterpolateResult runInterpolation(Query query);

  class InterpolateResult
  {
    private final boolean queryShouldRun;
    private final boolean interpolated;
    private final String msg;

    public InterpolateResult(boolean queryShouldRun, boolean interpolated, String msg)
    {
      this.queryShouldRun = queryShouldRun;
      this.interpolated = interpolated;
      this.msg = msg;
    }

    @JsonProperty("queryShouldRun")
    public boolean queryShouldRun()
    {
      return queryShouldRun;
    }

    @JsonProperty("interpolated")
    public boolean hasInterpolated()
    {
      return interpolated;
    }

    @JsonProperty("message")
    public String getMessage()
    {
      return this.msg;
    }
  }
}
