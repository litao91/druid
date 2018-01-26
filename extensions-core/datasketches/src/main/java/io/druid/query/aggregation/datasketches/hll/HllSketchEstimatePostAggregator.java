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
package io.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class HllSketchEstimatePostAggregator implements PostAggregator {
  private final String name;
  private final PostAggregator field;
  public static final byte HLL_SKETCH_ESTIMATE_CACHE_TYPE_ID = 0x21;

  @JsonCreator
  public HllSketchEstimatePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("field") PostAggregator field
  ) {
    this.name = Preconditions.checkNotNull(name, "Name is null");
    this.field = Preconditions.checkNotNull(field, "Field is null");
  }

  @Override
  public Set<String> getDependentFields() {
    Set<String> dependenctFields = Sets.newHashSet();
    dependenctFields.addAll(field.getDependentFields());
    return dependenctFields;
  }

  @Override
  public Comparator getComparator() {
    return Ordering.natural();
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators) {
    HllSketchHolder holder = (HllSketchHolder) field.compute(combinedAggregators);
    return holder.getEstimate();
  }


  @Override
  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public PostAggregator getField() {
    return field;
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators) {
    return this;
  }

  @Override
  public byte[] getCacheKey() {
    final CacheKeyBuilder builder = new CacheKeyBuilder(HLL_SKETCH_ESTIMATE_CACHE_TYPE_ID)
        .appendCacheable(field);
    return builder.build();
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + field.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HllSketchEstimatePostAggregator that = (HllSketchEstimatePostAggregator) o;

    if (!name.equals(that.name)) {
      return false;
    }

    return field.equals(that.field);
  }

  @Override
  public String toString() {

    return "HllSketchEstimatePostAggregator{" +
        "name='" + name + '\'' +
        ", field=" + field +
        "}";
  }
}
