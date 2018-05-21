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

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.Union;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import org.apache.commons.codec.binary.Base64;

import java.util.Comparator;

/**
 * A holder for a HllSketch Row data
 */
public class HllSketchHolder
{
  private final Object obj;
  private volatile HllSketch cachedSketch = null;
  private volatile Double cachedEstimate = null;

  private HllSketchHolder(Object obj)
  {
    Preconditions.checkArgument(obj instanceof HllSketch || obj instanceof Memory || obj instanceof Union,
        "Unkonwn HLL sketch representation type [%s]", obj.getClass().getName());
    this.obj = obj;
  }

  public static HllSketchHolder of(Object obj)
  {
    return new HllSketchHolder(obj);
  }

  private static final Comparator<HllSketch> HLL_SKETCH_COMPARATOR = (Comparator<HllSketch>) (o1, o2) -> Doubles.compare(o1.getEstimate(), o2.getEstimate());
  private static final Comparator<Memory> MEMORY_COMPARATOR = (Comparator<Memory>) (o1, o2) -> {
    int retVal = Longs.compare(o1.getCapacity(), o2.getCapacity());
    if (retVal == 0) {
      retVal = Longs.compare(o1.getLong(o2.getCapacity() - 8), o2.getLong(o2.getCapacity() - 8));
    }

    return retVal;
  };

  public static final Comparator<Object> COMPARATOR = Ordering.from((Comparator) (o1, o2) -> {
    HllSketchHolder h1 = (HllSketchHolder) o1;
    HllSketchHolder h2 = (HllSketchHolder) o2;
    if (h1.obj instanceof HllSketch || h1.obj instanceof Union) {
      if (h2.obj instanceof HllSketch || h2.obj instanceof Union) {
        return HLL_SKETCH_COMPARATOR.compare(h1.getHllSketch(), h2.getHllSketch());
      } else {
        return -1;
      }
    }

    if (h1.obj instanceof Memory) {
      if (h2.obj instanceof Memory) {
        return MEMORY_COMPARATOR.compare((Memory) h1.obj, (Memory) h2.obj);
      } else {
        return 1;
      }
    }
    throw new IAE("Unknown types [%s] and [%s]", h1.obj.getClass().getName(), h2.obj.getClass().getName());
  }).nullsFirst();


  /**
   * Update the given union with current sketch
   *
   * @param union
   */
  public void updateUnion(Union union)
  {
    if (obj instanceof Memory) {
      union.update(HllSketch.heapify((Memory) obj));
    } else {
      union.update(getHllSketch());
    }
  }

  public HllSketch getHllSketch()
  {
    if (cachedSketch != null) {
      return cachedSketch;
    }

    if (obj instanceof HllSketch) {
      cachedSketch = (HllSketch) obj;
    } else if (obj instanceof Union) {
      cachedSketch = ((Union) obj).getResult();
    } else if (obj instanceof Memory) {
      cachedSketch = (HllSketch) deserializeFromMemory((Memory) obj);
    } else {
      throw new ISE("Can't get sketch from object of type [%s]", obj.getClass().getName());
    }
    return cachedSketch;
  }

  /**
   * Get the estimated cardinality from the current sketch
   *
   * @return
   */
  public double getEstimate()
  {
    if (cachedEstimate == null) {
      cachedEstimate = getHllSketch().getEstimate();
    }
    return cachedEstimate.doubleValue();
  }

  /**
   * Combine two Holders (usually from Aggregator#get or BufferedAggregator#get), basically the union of two sketches
   *
   * @param o1
   * @param o2
   * @param lgK
   * @return
   */
  public static HllSketchHolder combine(Object o1, Object o2, int lgK)
  {
    HllSketchHolder holder1 = (HllSketchHolder) o1;
    HllSketchHolder holder2 = (HllSketchHolder) o2;
    // If one of them are union, merge the other side into the union
    // Otherwise we create a new Union and update it with the sketches.
    if (holder1.obj instanceof Union) {
      Union union = (Union) holder1.obj;
      holder2.updateUnion(union);
      // the obj of holder1 has ben updated thus the cache is no longer valid
      holder1.invalidateCache();
      return holder1;
    } else if (holder2.obj instanceof Union) {
      Union union = (Union) holder2.obj;
      holder1.updateUnion(union);
      holder2.invalidateCache();
      return holder2;
    } else {
      Union union = new Union(lgK);
      holder1.updateUnion(union);
      holder2.updateUnion(union);
      return HllSketchHolder.of(union);
    }
  }

  void invalidateCache()
  {
    cachedEstimate = null;
    cachedSketch = null;
  }

  /**
   * Generally used to deserialize the data from JSON
   *
   * @param serializedSketch
   * @return
   */
  public static HllSketchHolder deserialize(Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      return HllSketchHolder.of(deserializeFromBase64EncodedString((String) serializedSketch));
    } else if (serializedSketch instanceof byte[]) {
      return HllSketchHolder.of(deserializeFromByteArray((byte[]) serializedSketch));
    } else if (serializedSketch instanceof HllSketchHolder) {
      return (HllSketchHolder) serializedSketch;
    } else if (serializedSketch instanceof HllSketch
        || serializedSketch instanceof Memory) {
      return HllSketchHolder.of(serializedSketch);
    }

    throw new ISE(
        "Object is not of a type[%s] that can be deserialized to sketch.",
        serializedSketch.getClass()
    );
  }

  private static Object deserializeFromBase64EncodedString(String str)
  {
    return deserializeFromByteArray(Base64.decodeBase64(StringUtils.toUtf8(str)));
  }

  private static Object deserializeFromByteArray(byte[] data)
  {
    return deserializeFromMemory(Memory.wrap(data));
  }

  private static Object deserializeFromMemory(Memory mem)
  {
    return HllSketch.heapify(mem);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return this.getHllSketch().equals(((HllSketchHolder) o).getHllSketch());
  }
}