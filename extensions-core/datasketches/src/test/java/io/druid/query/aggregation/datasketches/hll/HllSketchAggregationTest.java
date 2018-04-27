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
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.Union;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.epinephelinae.GrouperTestUtil;
import io.druid.query.groupby.epinephelinae.TestColumnSelectorFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

@RunWith(Parameterized.class)
public class HllSketchAggregationTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public HllSketchAggregationTest(final GroupByQueryConfig config)
  {
    HllSketchModule m = new HllSketchModule();
    m.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(m.getJacksonModules(), config, tempFolder);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = Lists.newArrayList();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @Test
  public void testHllSketchDataIngestAndGroupBy() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(HllSketchAggregationTest.class.getClassLoader().getResource("simple_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("hll/hll_sketch_simple_test_data_aggregators.json"),
        0,
        Granularities.NONE,
        1000,
        readFileFromClasspathAsString("hll/simple_test_data_group_by_query.json")
    );
    List<Row> results = Sequences.toList(seq, Lists.newArrayList());
    Assert.assertEquals(5, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_3")
                    .put("hll_sketch_count", 38.00000349183915)
                    .put("hllSketchEstimatePostAgg", 38.00000349183915)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_1")
                    .put("hll_sketch_count", 42.00000427663378)
                    .put("hllSketchEstimatePostAgg", 42.00000427663378)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_2")
                    .put("hll_sketch_count", 42.00000427663378)
                    .put("hllSketchEstimatePostAgg", 42.00000427663378)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_4")
                    .put("hll_sketch_count", 42.00000427663378)
                    .put("hllSketchEstimatePostAgg", 42.00000427663378)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
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

  @Test
  public void testHllCardinalityOnSimpleColumn() throws Exception
  {
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
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_3")
                    .put("hll_sketch_count", 38.00000349183915)
                    .put("hllSketchEstimatePostAgg", 38.00000349183915)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_1")
                    .put("hll_sketch_count", 42.00000427663378)
                    .put("hllSketchEstimatePostAgg", 42.00000427663378)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_2")
                    .put("hll_sketch_count", 42.00000427663378)
                    .put("hllSketchEstimatePostAgg", 42.00000427663378)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_4")
                    .put("hll_sketch_count", 42.00000427663378)
                    .put("hllSketchEstimatePostAgg", 42.00000427663378)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
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

  private void assertAggregatorFactorySerde(AggregatorFactory agg) throws Exception
  {
    Assert.assertEquals(
        agg,
        helper.getObjectMapper().readValue(
            helper.getObjectMapper().writeValueAsString(agg),
            AggregatorFactory.class
        )
    );
  }

  @Test
  public void testHllSketchMergeAggregatorySerde() throws Exception
  {
    assertAggregatorFactorySerde(new HllSketchMergeAggregatorFactory(
        "name", "fieldName", 16, null));
    assertAggregatorFactorySerde(new HllSketchMergeAggregatorFactory(
        "name", "fieldName", 16, false));
    assertAggregatorFactorySerde(new HllSketchMergeAggregatorFactory(
        "name", "fieldName", 16, true));
  }

  private void assertPostAggregatorSerde(PostAggregator agg) throws Exception
  {
    Assert.assertEquals(
        agg,
        helper.getObjectMapper().readValue(
            helper.getObjectMapper().writeValueAsString(agg),
            PostAggregator.class
        )
    );
  }

  @Test
  public void testHllSketchEstimatePostAggregatorSerde() throws Exception
  {
    assertPostAggregatorSerde(
        new HllSketchEstimatePostAggregator(
            "name", new FieldAccessPostAggregator("name", "fieldName"))
    );
  }


  @Test
  public void testCacheKey()
  {
    final HllSketchMergeAggregatorFactory factory1 = new HllSketchMergeAggregatorFactory(
        "name",
        "fieldName",
        16,
        null
    );
    final HllSketchMergeAggregatorFactory factory2 = new HllSketchMergeAggregatorFactory(
        "name",
        "fieldName",
        16,
        null);
    final HllSketchMergeAggregatorFactory factory3 = new HllSketchMergeAggregatorFactory(
        "name",
        "fieldName",
        8,
        null
    );
    Assert.assertTrue(Arrays.equals(factory1.getCacheKey(), factory2.getCacheKey()));
    Assert.assertFalse(Arrays.equals(factory1.getCacheKey(), factory3.getCacheKey()));
  }

  @Test
  public void testRententionDataIngestAndGroupByQuery() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("retention_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("hll/simple_test_data_aggregators.json"),
        0,
        Granularities.NONE,
        5,
        readFileFromClasspathAsString("hll/retention_test_data_group_by_query.json")
    );

    List<Row> results = Sequences.toList(seq, Lists.<Row>newArrayList());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_1")
                    .put("p1_unique_country_day_1", 20.00000094374026)
                    .put("p1_unique_country_day_2", 20.00000094374026)
                    .put("p1_unique_country_day_3", 10.000000223517425)
                    .put("sketchEstimatePostAgg", 20.00000094374026)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            )
        ),
        results
    );
  }

  @Test
  public void testHllSketchAggregatorFactoryComparator()
  {
    Comparator<Object> comparator = HllSketchHolder.COMPARATOR;
    Assert.assertEquals(0, comparator.compare(null, null));

    Union union1 = new Union(15);
    union1.update("a");
    union1.update("b");
    HllSketch sketch1 = union1.getResult();

    Assert.assertEquals(-1, comparator.compare(null, HllSketchHolder.of(sketch1)));
    Assert.assertEquals(1, comparator.compare(HllSketchHolder.of(sketch1), null));

    Union union2 = new Union(15);
    union2.update("a");
    union2.update("b");
    union2.update("C");
    HllSketch sketch2 = union2.getResult();
    Assert.assertEquals(-1, comparator.compare(HllSketchHolder.of(sketch1), HllSketchHolder.of(sketch2)));
    Assert.assertEquals(-1, comparator.compare(HllSketchHolder.of(sketch1), HllSketchHolder.of(union2)));
    Assert.assertEquals(1, comparator.compare(HllSketchHolder.of(sketch2), HllSketchHolder.of(sketch1)));
    Assert.assertEquals(1, comparator.compare(HllSketchHolder.of(sketch2), HllSketchHolder.of(union1)));
    Assert.assertEquals(1, comparator.compare(HllSketchHolder.of(union2), HllSketchHolder.of(union1)));
    Assert.assertEquals(1, comparator.compare(HllSketchHolder.of(union2), HllSketchHolder.of(sketch1)));
  }

  @Test
  public void testRelocation()
  {
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    HllSketchHolder hllSketchHolder = HllSketchHolder.of(new HllSketch(16));
    HllSketch updateSketch = hllSketchHolder.getHllSketch();
    updateSketch.update(1);

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.<String, Object>of("sketch", hllSketchHolder)));
    HllSketchHolder[] holders = helper.runRelocateVerificationTest(
        new HllSketchMergeAggregatorFactory("sketch", "sketch", 16, false),
        columnSelectorFactory,
        HllSketchHolder.class
    );
    Assert.assertEquals(holders[0].getEstimate(), holders[1].getEstimate(), 0);
  }

  public final static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(HllSketchAggregationTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }


}
