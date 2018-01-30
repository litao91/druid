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
import com.yahoo.sketches.hll.Union;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import java.util.List;

public class HllSketchAggregator implements Aggregator {
  private final ObjectColumnSelector selector;
  private final int lgk;
  private Union union;

  /**
   * Combine the current column value to the united representation. In this case, a HllSketch Union
   */
  @Override
  public void aggregate() {
    Object update = selector.get();
    // if the input is null, nothing to do
    if (update == null) {
      return;
    }
    // if the aggregator is null, create a new one
    if (union == null) {
      initUnion();
    }

    // the real aggregate logic
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
      return HllSketchHolder.of(new HllSketch(lgk));
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

  /**
   * Update the given value into the hll sketch union
   *
   * @param union
   * @param update
   */
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
