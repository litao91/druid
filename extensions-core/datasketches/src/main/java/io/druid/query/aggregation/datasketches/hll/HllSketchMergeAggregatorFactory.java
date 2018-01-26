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
import io.druid.query.aggregation.AggregatorFactory;

import java.util.Collections;
import java.util.List;

public class HllSketchMergeAggregatorFactory extends HllSketchAggregatorFactory {
  private static final byte HLL_SKETCH_CACHE_TYPE_ID = 0x21;

  private final boolean isInputHllSketch;

  @JsonCreator
  public HllSketchMergeAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("lgk") Integer lgk,
      @JsonProperty("isInputHllSketch") Boolean isInputHllSketch
  ) {
    super(name, fieldName, lgk, HLL_SKETCH_CACHE_TYPE_ID);
    this.isInputHllSketch = isInputHllSketch.booleanValue();
  }

  @Override
  public AggregatorFactory getCombiningFactory() {
    return new HllSketchMergeAggregatorFactory(name, fieldName, lgk, false);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns() {
    return Collections.singletonList(new HllSketchMergeAggregatorFactory(fieldName, fieldName, lgk, isInputHllSketch));
  }

  @Override
  public Object finalizeComputation(Object object) {
    HllSketchHolder holder = (HllSketchHolder) object;
    return holder.getEstimate();
  }

  @Override
  public String getTypeName() {
    if (isInputHllSketch) {
      return HllSketchModule.HLL_SKETCH_MERGE_AGG;
    } else {
      return HllSketchModule.HLL_SKETCH_BUILD_AGG;
    }
  }

  @Override
  public boolean equals(Object o) {

    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    HllSketchMergeAggregatorFactory that = (HllSketchMergeAggregatorFactory) o;

    return isInputHllSketch == that.isInputHllSketch;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (isInputHllSketch ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "SketchMergeAggregatorFactory{"
        + "fieldName=" + fieldName
        + ", name=" + name
        + ", lgk=" + lgk
        + ", isInputHllSketch=" + isInputHllSketch
        + "}";
  }
}
