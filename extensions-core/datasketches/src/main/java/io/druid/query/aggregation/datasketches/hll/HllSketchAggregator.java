/*
 */
package io.druid.query.aggregation.datasketches.hll;

import com.yahoo.sketches.hll.Union;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import java.util.List;

public class HllSketchAggregator implements Aggregator {
  private final ObjectColumnSelector selector;
  private final int lgk;
  private Union union;

  @Override
  public void aggregate() {
    Object update = selector.get();
    if (update == null) {
      return;
    }
    if (union == null) {
      initUnion();
    }
    updateUnion(union, update);
  }

  public HllSketchAggregator(ObjectColumnSelector selector, int lgk) {
    this.selector = selector;
    this.lgk = lgk;
  }

  private void initUnion() {
    union = new Union(this.lgk);
  }

  @Override
  public void reset() {
    union.reset();
  }

  @Override
  public Object get() {
    if (union == null) {
      return new Union(lgk);
    }
    return HllSketchHolder.of(union.getResult());
  }

  @Override
  public float getFloat() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() {
    union = null;
  }

  @Override
  public long getLong() {
    throw new UnsupportedOperationException("Not implemented");
  }

  static void updateUnion(Union union, Object update) {
    if (update instanceof HllSketchHolder) {
      ((HllSketchHolder) update).updateUnion(union);
    } else if (update instanceof String) {
      union.update((String) update);
    } else if (update instanceof byte[]) {
      union.update((byte[]) update);
    } else if (update instanceof Double) {
      union.update(((Double) update));
    } else if (update instanceof Integer || update instanceof Long) {
      union.update(((Number) update).longValue());
    } else if (update instanceof int[]) {
      union.update((int[]) update);
    } else if (update instanceof long[]) {
      union.update((long[]) update);
    } else if (update instanceof List) {
      for (Object entry : (List) update) {
        union.update(entry.toString());
      }
    } else {
      throw new ISE("Illegal type received while theta sketch merging [%s]", update.getClass());
    }
  }
}
