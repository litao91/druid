package io.druid.query.aggregation.datasketches.hll;

import com.google.common.base.Preconditions;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;
import java.util.List;

public abstract class HllSketchAggregatorFactory extends AggregatorFactory {
  public static final int DEFAULT_MAX_LGK = 21;

  protected final String name;
  protected final String fieldName;
  protected final int lgk;
  private final byte cacheId;

  public HllSketchAggregatorFactory(String name, String fieldName, Integer lgk, byte cacheId) {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.lgk = lgk == null ? DEFAULT_MAX_LGK : lgk;
    this.cacheId = cacheId;
  }


  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory) {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {

    } else {
      return new HllSketchAggregator(selector, lgk);
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
    return null;
  }

  @Override
  public Comparator getComparator() {
    return null;
  }

  @Override
  public Object combine(Object lhs, Object rhs) {
    return null;
  }

  @Override
  public AggregatorFactory getCombiningFactory() {
    return null;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns() {
    return null;
  }

  @Override
  public Object deserialize(Object object) {
    return null;
  }

  @Override
  public Object finalizeComputation(Object object) {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public List<String> requiredFields() {
    return null;
  }

  @Override
  public String getTypeName() {
    return null;
  }

  @Override
  public int getMaxIntermediateSize() {
    return 0;
  }

  @Override
  public byte[] getCacheKey() {
    return new byte[0];
  }
}
