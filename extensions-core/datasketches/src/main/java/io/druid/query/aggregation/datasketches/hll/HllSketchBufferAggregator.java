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

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.Union;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ObjectColumnSelector;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class HllSketchBufferAggregator implements BufferAggregator {
  private final ObjectColumnSelector selector;
  private final int lgk;
  private final int maxIntermediateSize;

  /**
   * Map the position to the Union object inside a byteBuffer
   */
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<Union>> unions = new IdentityHashMap<>();

  /**
   * map the byte buffer to the wrapped Writable Memory
   */
  private final IdentityHashMap<ByteBuffer, WritableMemory> memCache = new IdentityHashMap<>();

  public HllSketchBufferAggregator(ObjectColumnSelector selector, int lgK, int maxIntermediateSize) {
    this.selector = selector;
    this.lgk = lgK;
    this.maxIntermediateSize = maxIntermediateSize;
  }


  /**
   * Initialize the buffer in the given location.
   *
   * @param buf      byte buffer to initialize
   * @param position offset within the byte buffer for initialization
   */
  @Override
  public void init(ByteBuffer buf, int position) {
    // at the time of initialization, the memory is not wrapped.
    createNewUnion(buf, position, false);
  }

  private Union getOrCreateUnion(ByteBuffer buf, int position) {
    Int2ObjectMap<Union> unionMap = unions.get(buf);
    Union union = unionMap != null ? unionMap.get(position) : null;
    if (union != null) {
      return union;
    }
    return createNewUnion(buf, position, true);
  }

  /**
   * Create a hll/Union object in a given position of a buf
   *
   * @param buf
   * @param position
   * @param wrapped  whether the memory is Union image
   * @return
   */
  private Union createNewUnion(ByteBuffer buf, int position, boolean wrapped) {
    // the region to be written to
    WritableMemory mem = getMemory(buf).writableRegion(position, maxIntermediateSize);
    // if it's wrapped, we use the image, otherwise we create a new union on top of it.
    Union union = wrapped ? Union.writableWrap(mem) : new Union(lgk, mem);

    // Cache the buffer
    Int2ObjectMap<Union> unionMap = unions.get(buf);
    // if the buf is not in the map
    if (unionMap == null) {
      unionMap = new Int2ObjectOpenHashMap<>();
      unions.put(buf, unionMap);
    }
    // map the object to the concrete object
    unionMap.put(position, union);
    return union;
  }

  private WritableMemory getMemory(ByteBuffer buffer) {
    WritableMemory mem = memCache.get(buffer);
    if (mem == null) {
      mem = WritableMemory.wrap(buffer);
      memCache.put(buffer, mem);
    }
    return mem;
  }

  /**
   * Get the union in the given position, update it (since the memory is wrapped, it will automatically update
   * the underlying ByteBuffer
   *
   * @param buf byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the current aggregate value is stored
   */
  @Override
  public void aggregate(ByteBuffer buf, int position) {
    Object update = selector.get();
    if (update == null) {
      return;
    }

    Union union = getOrCreateUnion(buf, position);
    HllSketchAggregator.updateUnion(union, update);
  }

  @Override
  public Object get(ByteBuffer buf, int position) {
    Int2ObjectMap<Union> unionMap = unions.get(buf);
    Union union = unionMap != null ? unionMap.get(position) : null;
    if (union == null) {
      return HllSketchHolder.of(new HllSketch(lgk));
    }
    return HllSketchHolder.of(union.getResult());
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
    unions.clear();
    memCache.clear();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector) {
    inspector.visit("selector", selector);
  }

  /**
   * This method tells the BufferAggregator that the cached object at a certain location has been located to a different
   * location
   *
   * @param oldPosition old position of a cached object before aggregation buffer relocates to a new ByteBuffer.
   * @param newPosition new position of a cached object after aggregation buffer relocates to a new ByteBuffer.
   * @param oldBuffer old aggregation buffer.
   * @param newBuffer new aggregation buffer.
   */
  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer) {
    createNewUnion(newBuffer, newPosition, true);
    Int2ObjectMap<Union> unionMap = unions.get(oldBuffer);
    if (unionMap != null) {
      unionMap.remove(oldPosition);
      if (unionMap.isEmpty()) {
        unions.remove(oldBuffer);
        memCache.remove(oldBuffer);
      }
    }
  }
}
