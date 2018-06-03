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
package io.druid.segment.transform;

import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

public class ShadowInputRow implements InputRow
{
  private final InputRow delegate;
  Map<String, Object> shadow;

  public ShadowInputRow(InputRow delegate, Map<String, Object> shadow)
  {
    this.delegate = delegate;
    if (shadow.containsKey(Column.TIME_COLUMN_NAME)) {
      throw new IllegalArgumentException("Timestamp collumn can't be shadowed");
    }
    this.shadow = shadow;
  }

  @Override
  public List<String> getDimensions()
  {
    return delegate.getDimensions();
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return delegate.getTimestampFromEpoch();
  }

  @Override
  public DateTime getTimestamp()
  {
    return delegate.getTimestamp();
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    if (shadow.containsKey(dimension)) {
      return Rows.objectToStrings(shadow.get(dimension));
    } else {
      return delegate.getDimension(dimension);
    }
  }

  @Override
  public Object getRaw(String dimension)
  {
    if (shadow.containsKey(dimension)) {
      return shadow.get(dimension);
    } else {
      return delegate.getRaw(dimension);
    }
  }

  @Override
  public Number getMetric(String metric)
  {
    if (shadow.containsKey(metric)) {
      return Rows.objectToNumber(metric, shadow.get(metric));
    } else {
      return delegate.getMetric(metric);
    }
  }

  @Override
  public int compareTo(Row o)
  {
    return delegate.compareTo(o);
  }

  @Override
  public int hashCode()
  {
    int result = delegate.hashCode();
    result = 31 * result + shadow.hashCode();
    return result;
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
    ShadowInputRow that = (ShadowInputRow) o;

    if (!this.delegate.equals(that.delegate)) {
      return false;
    }

    if (!this.shadow.equals(that.shadow)) {
      return false;
    }

    return true;
  }
}
