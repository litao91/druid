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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;

import javax.annotation.Nullable;
import java.util.List;

public class Splitter
{
  private final List<RowSplitFunction> splits = Lists.newArrayList();

  Splitter(final TransformSpec transformSpec)
  {
    for (Split s : transformSpec.getSplits()) {
      splits.add(s.getSplitRowFunction());
    }
  }

  @Nullable
  public List<InputRow> split(@Nullable final InputRow row)
  {
    if (splits.isEmpty()) {
      return ImmutableList.of(row);
    }

    List<InputRow> out = Lists.newArrayList();

    for (RowSplitFunction s : splits) {
      Iterable<InputRow> rows = s.eval(row);
      for (InputRow r : rows) {
        out.add(r);
      }
    }
    return out;
  }
}
