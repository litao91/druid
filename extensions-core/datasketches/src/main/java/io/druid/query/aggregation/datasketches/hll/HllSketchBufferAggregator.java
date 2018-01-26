/*
 */
package io.druid.query.aggregation.datasketches.hll;

import com.yahoo.memory.WritableMemory;
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
  private final int lgK;
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
    this.lgK = lgK;
    this.maxIntermediateSize = maxIntermediateSize;
  }


  /**
   * @param buf      byte buffer to initialize
   * @param position offset within the byte buffer for initialization
   */
  @Override
  public void init(ByteBuffer buf, int position) {
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
    Union union = wrapped ? Union.writableWrap(mem) : new Union(lgK, mem);
    Int2ObjectMap<Union> unionMap = unions.get(buf);
    // if the buf is not in the map
    if (unionMap == null) {
      unionMap = new Int2ObjectOpenHashMap<>();
      unions.put(buf, unionMap);
    }
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
      return HllSketchHolder.EMPTY;
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
