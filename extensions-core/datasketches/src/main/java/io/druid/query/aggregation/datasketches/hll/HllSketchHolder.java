package io.druid.query.aggregation.datasketches.hll;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import org.apache.commons.codec.binary.Base64;

import java.util.Comparator;

/**
 * A holder for a HllSketch Row data
 */
public class HllSketchHolder {

  private final Object obj;
  private volatile HllSketch cachedSketch = null;
  private volatile Double cachedEstimate = null;

  private HllSketchHolder(Object obj) {
    Preconditions.checkArgument(obj instanceof HllSketch || obj instanceof Memory,
        "Unkonwn HLL sketch representation type [%s]", obj.getClass().getName());
    this.obj = obj;
  }


  public HllSketch getSketch() {
    if (cachedSketch != null) {
      return cachedSketch;
    }

    if (obj instanceof HllSketch) {
      cachedSketch = (HllSketch) obj;
    }  else if (obj instanceof Memory) {
      cachedSketch = deserializeFromMemory((Memory) obj);
    } else {
      throw new ISE("Can't get sketch from object of type [%s]", obj.getClass().getName());
    }
    return cachedSketch;
  }


  public double getEstimate() {
    if (cachedEstimate == null) {
      cachedEstimate = getSketch().getEstimate();
    }
    return cachedEstimate.doubleValue();
  }

  private static HllSketch deserializeFromBase64EncodedString(String str) {
    return deserializedFromByteArray(Base64.decodeBase64(StringUtils.toUtf8(str)));
  }

  private static HllSketch deserializedFromByteArray(byte[] data) {
    return HllSketch.heapify(data);
  }

  private static HllSketch deserializeFromMemory(Memory mem) {
    return HllSketch.heapify(mem);
  }
}
