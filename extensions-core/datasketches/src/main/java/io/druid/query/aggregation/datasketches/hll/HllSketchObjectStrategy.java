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

  @Override
  public Object fromByteBuffer(ByteBuffer buffer, int numBytes) {
    if (numBytes == 0) {
      return HllSketchHolder.EMPTY;
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
