package io.druid.query.aggregation.datasketches.hll;

import io.druid.data.input.InputRow;
import io.druid.segment.serde.ComplexMetricExtractor;

public class HllSketchBuildComplexMetricSerde extends HllSketchMergeComplexMetricSerde {
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
        return inputRow.getRaw(metricName);
      }
    };
  }
}
