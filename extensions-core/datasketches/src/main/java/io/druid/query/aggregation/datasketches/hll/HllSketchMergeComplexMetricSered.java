/*
 */
package io.druid.query.aggregation.datasketches.hll;

import io.druid.data.input.InputRow;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;

public class HllSketchMergeComplexMetricSered extends ComplexMetricSerde {
  private HllSketchObjectStrategy strategy = new HllSketchObjectStrategy();

  @Override
  public String getTypeName() {
    return HllSketchModule.HLL_SKETCH;
  }

  @Override
  public ComplexMetricExtractor getExtractor() {
    return new ComplexMetricExtractor() {
      @Override
      public Class<?> extractedClass() {
        return Object.class;
      }

      @Override
      public Object extractValue(InputRow inputRow, String metricName) {
        final Object object = inputRow.getRaw(metricName);
        if (object == null) {
          return object;
        }
        return HllSketchHolder.deserialize(object);
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder) {
    GenericIndexed<HllSketchMergeComplexMetricSered> ge = GenericIndexed.read(buffer, strategy, builder.getFileMapper());
    builder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), ge));
  }

  @Override
  public ObjectStrategy getObjectStrategy() {
    return strategy;
  }
}
