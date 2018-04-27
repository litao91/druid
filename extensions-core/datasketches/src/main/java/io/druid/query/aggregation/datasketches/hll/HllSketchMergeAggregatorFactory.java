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
import io.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Represets columne aggregation
 */
public class HllSketchMergeAggregatorFactory extends AggregatorFactory
{
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
  )
  {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.lgk = lgk == null ? DEFAULT_MAX_LGK : lgk;
    this.isInputHllSketch = isInputHllSketch == null ? false : isInputHllSketch.booleanValue();
    this.cacheId = HLL_SKETCH_CACHE_TYPE_ID;
  }

  /**
   * Create factory for combining get output of HllSketchMergeAggregatorFactory
   * Since we don't alternate the type, the same factory is used. (HllSketchHolder)
   *
   * @return
   */
  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HllSketchMergeAggregatorFactory(name, name, lgk, false);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new HllSketchMergeAggregatorFactory(fieldName, fieldName, lgk, isInputHllSketch));
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    HllSketchHolder holder = (HllSketchHolder) object;
    return holder.getEstimate();
  }

  @Override
  public String getTypeName()
  {
    return HllSketchModule.HLL_SKETCH_BUILD_AGG;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HllSketchMergeAggregatorFactory that = (HllSketchMergeAggregatorFactory) o;

    if (lgk != that.lgk) {
      return false;
    }

    if (cacheId != that.cacheId) {
      return false;
    }

    if (!name.equals(that.name)) {
      return false;
    }

    if (!fieldName.equals(that.fieldName)) {
      return false;
    }

    return isInputHllSketch == that.isInputHllSketch;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (isInputHllSketch ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "HllSketchMergeAggregatorFactory{"
        + "fieldName=" + fieldName
        + ", name=" + name
        + ", lgk=" + lgk
        + ", isInputHllSketch=" + isInputHllSketch
        + "}";
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    BaseObjectColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
    return new HllSketchAggregator(selector, lgk);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    BaseObjectColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
    return new HllSketchBufferAggregator(selector, lgk, getMaxIntermediateSize());
  }

  @Override
  public Comparator getComparator()
  {
    return HllSketchHolder.COMPARATOR;
  }

  /**
   * Combine the output from HllAggregator#get or HllBufferedAggregator#get
   * They should of type HllSketchHolder. The real instance type could be
   * Union or HllSketch
   *
   * @param lhs The left hand side of the combine
   * @param rhs The right hand side of the combine
   * @return
   */
  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return HllSketchHolder.combine(lhs, rhs, lgk);
  }

  /**
   * Deserialize the data from json
   *
   * @param object the object to deserialize
   * @return
   */
  @Override
  public Object deserialize(Object object)
  {
    return HllSketchHolder.deserialize(object);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public int getLgk()
  {
    return lgk;
  }

  @JsonProperty
  public boolean getIsInputHllSketch()
  {
    return isInputHllSketch;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  /**
   * Returns the maximum size in bytes that this union operator can grow to given a lgK.
   */
  @Override
  public int getMaxIntermediateSize()
  {
    return Union.getMaxSerializationBytes(lgk);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(1 + Ints.BYTES + fieldNameBytes.length)
        .put(cacheId)
        .putInt(lgk)
        .put(fieldNameBytes)
        .array();
  }
}
