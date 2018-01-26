package io.druid.query.aggregation.datasketches.hll;

import io.druid.query.aggregation.AggregatorFactory;

import java.util.List;

public class HllSketchMergeAggregatorFactory extends HllSketchAggregatorFactory {

  public HllSketchMergeAggregatorFactory(String name, String fieldName, Integer lgk, byte cacheId) {
    super(name, fieldName, lgk, cacheId);
  }

  @Override
  public AggregatorFactory getCombiningFactory() {
    return null;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns() {
    return null;
  }

  @Override
  public Object finalizeComputation(Object object) {
    return null;
  }

  @Override
  public String getTypeName() {
    return null;
  }
}
