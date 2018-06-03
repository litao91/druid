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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;

import java.util.List;

public class SeparatorSplit implements Split
{
  private final String fieldName;
  private final String separator;

  @JsonCreator
  public SeparatorSplit(
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("separator") final String separator
  )
  {
    this.fieldName = fieldName;
    this.separator = separator;
  }

  @JsonProperty("separator")
  public String getSeparator()
  {
    return separator;
  }

  @JsonProperty("fieldName")
  @Override
  public String getFieldName()
  {
    return this.fieldName;
  }

  @Override
  public RowSplitFunction getSplitRowFunction()
  {
    return row -> {
      List<InputRow> splittedRows = Lists.newArrayList();
      List<String> strings = Rows.objectToStrings(row.getRaw(fieldName));
      for (String str : strings) {
        for (String splitted : str.split(separator)) {
          splittedRows.add(new ShadowInputRow(row, ImmutableMap.of(fieldName, splitted)));
        }
      }
      return splittedRows;
    };
  }
}
