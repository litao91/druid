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
import io.druid.query.aggregation.Aggregator;

public class EmptyHllSketchAggregator implements Aggregator {
  private final int lgk;
  public EmptyHllSketchAggregator(int lgk) {
    this.lgk = lgk;
  }

  @Override
  public void aggregate() {

  }

  @Override
  public void reset() {

  }

  /**
   * For initialization, we create a new empty union
   * @return a empty union
   */
  @Override
  public Object get() {
    // for initialization, we create a new HllSketch with max accuracy possible
    return HllSketchHolder.of(new HllSketch(lgk));

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
