/*
 */
package io.druid.query.aggregation.datasketches.hll;

import io.druid.query.aggregation.BufferAggregator;

import java.nio.ByteBuffer;

public class EmptyHllSketchBufferAggregator implements BufferAggregator {
  private static final EmptyHllSketchBufferAggregator INSTANCE = new EmptyHllSketchBufferAggregator();

  public static EmptyHllSketchBufferAggregator instance() {
    return INSTANCE;
  }

  @Override
  public void init(ByteBuffer buf, int position) {
  }

  @Override
  public void aggregate(ByteBuffer buf, int position) {

  }

  @Override
  public Object get(ByteBuffer buf, int position) {
    return HllSketchHolder.EMPTY;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(ByteBuffer buf, int position) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() {

  }
}
