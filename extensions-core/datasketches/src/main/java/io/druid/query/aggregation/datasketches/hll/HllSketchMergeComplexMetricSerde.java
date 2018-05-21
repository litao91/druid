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

import com.yahoo.sketches.hll.HllSketch;
import io.druid.data.input.InputRow;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;

public class HllSketchMergeComplexMetricSerde extends ComplexMetricSerde
{
  private HllSketchObjectStrategy strategy = new HllSketchObjectStrategy();

  @Override
  public String getTypeName()
  {
    return HllSketchModule.HLL_SKETCH;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return Object.class;
      }

      @Override
      public Object extractValue(InputRow inputRow, String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        if (object == null) {
          return object;
        }
        return HllSketchHolder.deserialize(object);
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    GenericIndexed<HllSketch> ge = GenericIndexed.read(buffer, strategy, builder.getFileMapper());
    builder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), ge));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return strategy;
  }

  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }

}