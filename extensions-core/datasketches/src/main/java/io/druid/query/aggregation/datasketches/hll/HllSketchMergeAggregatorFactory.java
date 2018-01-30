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
import com.google.common.primitives.Ints;
import com.yahoo.sketches.hll.Union;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Represets columne aggregation
 */
public class HllSketchMergeAggregatorFactory extends AggregatorFactory {
  private static final byte HLL_SKETCH_CACHE_TYPE_ID = 0x21;
  public static final int DEFAULT_MAX_LGK = 21;

  protected final String name;
  protected final String fieldName;
  protected final int lgk;
  private final byte cacheId;

  private final boolean isInputHllSketch;

  @JsonCreator
  public HllSketchMergeAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("lgk") Integer lgk,
      @JsonProperty("isInputHllSketch") Boolean isInputHllSketch
  ) {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.lgk = lgk == null ? DEFAULT_MAX_LGK : lgk;
    this.isInputHllSketch = isInputHllSketch.booleanValue();
    this.cacheId = HLL_SKETCH_CACHE_TYPE_ID;
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
    // Depending on the input type, we use different sedrd strategy
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

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory) {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      // this will return a empty HllSketchHolder with
      return new EmptyHllSketchAggregator();
    } else {
      return new HllSketchAggregator(selector, lgk);
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      return EmptyHllSketchBufferAggregator.instance();
    } else {
      return new HllSketchBufferAggregator(selector, lgk, getMaxIntermediateSize());
    }
  }

  @Override
  public Comparator getComparator() {
    return HllSketchHolder.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs) {
    return HllSketchHolder.combine(lhs, rhs, lgk);
  }

  @Override
  public Object deserialize(Object object) {
    return HllSketchHolder.deserialize(object);
  }

  @Override
  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getFieldName() {
    return fieldName;
  }

  @JsonProperty
  public int getLgk() {
    return lgk;
  }

  @Override
  public List<String> requiredFields() {
    return Collections.singletonList(fieldName);
  }

  @Override
  public int getMaxIntermediateSize() {
    return Union.getMaxSerializationBytes(lgk);
  }

  @Override
  public byte[] getCacheKey() {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(1 + Ints.BYTES + fieldNameBytes.length)
        .put(cacheId)
        .putInt(lgk)
        .put(fieldNameBytes)
        .array();
  }
}
