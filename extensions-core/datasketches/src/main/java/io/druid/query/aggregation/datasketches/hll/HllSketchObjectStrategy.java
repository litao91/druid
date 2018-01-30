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

import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;
import io.druid.java.util.common.IAE;
import io.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;

public class HllSketchObjectStrategy implements ObjectStrategy {
  private static final byte[] EMPTY_BYTES = new byte[]{};

  @Override
  public Class getClazz() {
    return Object.class;
  }

  /**
   * Convert from underlying bytebuffer to HllSketchHolder
   *
   * @param buffer buffer to read value from
   * @param numBytes number of bytes used to store the value, starting at buffer.position()
   * @return
   */
  @Override
  public Object fromByteBuffer(ByteBuffer buffer, int numBytes) {
    if (numBytes == 0) {
      return HllSketchHolder.of(new HllSketch(21));
    }
    return HllSketchHolder.of(Memory.wrap(buffer).region(buffer.position(), numBytes));
  }

  @Override
  public byte[] toBytes(Object obj) {
    if (obj instanceof HllSketchHolder) {
      HllSketch hllSketch = ((HllSketchHolder) obj).getHllSketch();
      if (hllSketch.isEmpty()) {
        return EMPTY_BYTES;
      }
      return hllSketch.toCompactByteArray();
    } else if (obj == null) {
      return EMPTY_BYTES;
    } else {
      throw new IAE("Unknown class[%s], toString[%s]", obj.getClass(), obj);
    }
  }

  @Override
  public int compare(Object o1, Object o2) {
    return HllSketchHolder.COMPARATOR.compare(o1, o2);
  }
}
