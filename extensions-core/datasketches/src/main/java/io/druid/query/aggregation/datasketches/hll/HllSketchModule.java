package io.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;

import java.util.Arrays;
import java.util.List;


public class HllSketchModule implements DruidModule {
  public static final String HLL_SKETCH = "hllSketch";

  public static final String HLL_SKETCH_MERGE_AGG = "hllSketchMerge";
  public static final String HLL_SKETCH_BUILD_AGG = "hllSketchBuild";

  public static final String HLL_SKETCH_ESTIMATE_POST_AGG = "hllSketchEstimate";


  @Override
  public void configure(Binder binder) {
    if (ComplexMetrics.getSerdeForType(HLL_SKETCH) == null) {
      ComplexMetrics.registerSerde(HLL_SKETCH, new HllSketchMergeComplexMetricSered());
    }

    if (ComplexMetrics.getSerdeForType(HLL_SKETCH_MERGE_AGG) == null) {
      ComplexMetrics.registerSerde(HLL_SKETCH_MERGE_AGG, new HllSketchMergeComplexMetricSered());
    }

    if (ComplexMetrics.getSerdeForType(HLL_SKETCH_BUILD_AGG) == null) {
      ComplexMetrics.registerSerde(HLL_SKETCH_BUILD_AGG, new HllSketchMergeComplexMetricSered());
    }
  }

  @Override
  public List<? extends Module> getJacksonModules() {
    return Arrays.asList(
        new SimpleModule("HllSketchModule")
        .addSerializer(HllSketchHolder.class, new HllSketchHolderJsonSerializer())
    );
  }
}
