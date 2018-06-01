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

/**
 * A row split is part of a {@link TransformSpec}. Split allow splitting a input row to multiple
 * rows. The splitted dimension will be of the same name as the original input row.
 */
public interface Split
{
  /**
   * Return the field name for this split to apply to
   * @return
   */
  String getName();

  /**
   * Returns the function for this transform
   */
  RowSplitFunction getSplitRowFunction();

}
