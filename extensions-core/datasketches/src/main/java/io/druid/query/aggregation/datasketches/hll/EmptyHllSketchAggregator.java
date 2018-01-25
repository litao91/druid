package io.druid.query.aggregation.datasketches.hll;

import io.druid.query.aggregation.Aggregator;

public class EmptyHllSketchAggregator implements Aggregator {
  @Override
  public void aggregate() {

  }

  @Override
  public void reset() {

  }

  @Override
  public Object get() {
    return HllSketchHolder.EMPTY;

  }

  @Override
  public float getFloat() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() {

  }

  @Override
  public long getLong() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
