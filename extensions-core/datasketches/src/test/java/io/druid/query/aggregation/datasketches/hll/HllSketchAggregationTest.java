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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class HllSketchAggregationTest {
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public HllSketchAggregationTest(final GroupByQueryConfig config) {
    HllSketchModule m = new HllSketchModule();
    m.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(m.getJacksonModules(), config, tempFolder);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder() {
    final List<Object[]> constructors = Lists.newArrayList();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @Test
  public void testHllCardinalityOnSimpleColumn() throws Exception {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(HllSketchAggregationTest.class.getClassLoader().getResource("simple_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser2.json"),
        "["
            + "  {"
            + "    \"type\": \"count\","
            + "    \"name\": \"count\""
            + "  }"
            + "]",
        0,
        Granularities.NONE,
        1000,
        readFileFromClasspathAsString("hll/simple_test_data_group_by_query.json")
    );

    List<Row> results = Sequences.toList(seq, Lists.<Row>newArrayList());
    Assert.assertEquals(5, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTime.parse("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_3")
                    .put("hll_sketch_count", 38.00000349183915)
                    .put("hllSketchEstimatePostAgg", 38.00000349183915)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTime.parse("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_1")
                    .put("hll_sketch_count", 42.00000427663378)
                    .put("hllSketchEstimatePostAgg", 42.00000427663378)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTime.parse("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_2")
                    .put("hll_sketch_count", 42.00000427663378)
                    .put("hllSketchEstimatePostAgg", 42.00000427663378)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTime.parse("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_4")
                    .put("hll_sketch_count", 42.00000427663378)
                    .put("hllSketchEstimatePostAgg", 42.00000427663378)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTime.parse("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_5")
                    .put("hll_sketch_count", 42.00000427663378)
                    .put("hllSketchEstimatePostAgg", 42.00000427663378)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            )
      ),
      results
    );
  }

  public final static String readFileFromClasspathAsString(String fileName) throws IOException {
    return Files.asCharSource(
        new File(HllSketchAggregationTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
