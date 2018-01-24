package io.druid.query.aggregation.datasketches.hll;

import com.yahoo.sketches.hll.HllSketch;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;

public class HllSketchMergeComplexMetricSered extends ComplexMetricSerde {


  @Override
  public String getTypeName() {
    return null;
  }

  @Override
  public ComplexMetricExtractor getExtractor() {
    return null;
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder) {

  }

  @Override
  public ObjectStrategy getObjectStrategy() {
    return null;
  }
}
