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
package io.druid.server.router;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.segment.TestHelper;
import io.druid.server.router.interpolator.BlackWhiteListQueryInterpolator;
import io.druid.server.router.interpolator.QueryInterpolator;
import io.druid.server.router.interpolator.QueryIntervalDurationInterpolator;
import com.google.common.collect.ImmutableList;
import io.druid.server.router.setup.QueryProxyBehaviorConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class InterpolatorTest
{
  @Test
  public void testSerdeQueryInterpolatorSingle() throws Exception
  {
    QueryInterpolator interpolator = new QueryIntervalDurationInterpolator(
        ImmutableList.of("a", "b", "c"),
        ImmutableList.of("b", "c", "d", "e"),
        100
    );
    ObjectMapper mapper = new DefaultObjectMapper();
    String serialized = mapper.writeValueAsString(interpolator);

    QueryIntervalDurationInterpolator deserialized = mapper.reader(QueryInterpolator.class).readValue(serialized);
    Assert.assertEquals(interpolator, deserialized);

    BlackWhiteListQueryInterpolator bwInterpolator = (BlackWhiteListQueryInterpolator) interpolator;
    Assert.assertTrue(bwInterpolator.shouldApply("a"));
    Assert.assertFalse(bwInterpolator.shouldApply("b"));
    Assert.assertFalse(bwInterpolator.shouldApply("f"));
  }

  @Test
  public void testEmpytBWList() throws Exception
  {
    QueryIntervalDurationInterpolator interpolator = new QueryIntervalDurationInterpolator(
        ImmutableList.of(),
        ImmutableList.of("b", "c", "d", "e"),
        100
    );
    Assert.assertTrue(interpolator.shouldApply("f"));
    Assert.assertFalse(interpolator.shouldApply("c"));

    QueryIntervalDurationInterpolator interpolator2 = new QueryIntervalDurationInterpolator(
        ImmutableList.of("b", "c", "d", "e"),
        ImmutableList.of(),
        100
    );
    Assert.assertTrue(interpolator2.shouldApply("b"));
    Assert.assertFalse(interpolator2.shouldApply("f"));
  }

  @Test
  public void testQueryIntervalDurationInterpolator() throws IOException
  {
    String queryStr = "{\n" +
                      "    \"aggregations\": [\n" +
                      "        {\n" +
                      "            \"fieldName\": \"count\", \n" +
                      "            \"type\": \"longSum\", \n" +
                      "            \"name\": \"count\"\n" +
                      "        }\n" +
                      "    ], \n" +
                      "    \"intervals\": [\n" +
                      "        \"2018-02-23T00:00:00/2018-02-24T00:00:00\"\n" +
                      "    ], \n" +
                      "    \"dataSource\": \"audience_stat_gender\", \n" +
                      "    \"context\": {\n" +
                      "        \"timeout\": 25000\n" +
                      "    }, \n" +
                      "    \"granularity\": \"all\", \n" +
                      "    \"queryType\": \"groupBy\"\n" +
                      "}";
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    Query q = mapper.readValue(queryStr, Query.class);
    QueryIntervalDurationInterpolator interpolator = new QueryIntervalDurationInterpolator(
        ImmutableList.of(),
        ImmutableList.of(),
        100
    );
    QueryInterpolator.InterpolateResult r = interpolator.runInterpolation(q, null);
    Assert.assertFalse(r.queryShouldRun());

    interpolator = new QueryIntervalDurationInterpolator(
        ImmutableList.of(),
        ImmutableList.of(),
        86500000
    );

    r = interpolator.runInterpolation(q, null);
    Assert.assertTrue(r.queryShouldRun());
  }

  @Test
  public void testSerdeQueryInterpolator() throws Exception
  {
    List<QueryInterpolator> expected = ImmutableList.of(
        new QueryIntervalDurationInterpolator(
            ImmutableList.of("a", "b", "c"),
            ImmutableList.of("b", "c", "d", "e"),
            100
        ),
        new QueryIntervalDurationInterpolator(
            ImmutableList.of("a1", "b1", "c1"),
            ImmutableList.of("b1", "c1", "d1", "e1"),
            1000
        )
    );
    ObjectMapper mapper = new DefaultObjectMapper();
    String serialized = mapper.writerWithType(new TypeReference<List<QueryInterpolator>>()
    {
    }).writeValueAsString(expected);
    List<QueryInterpolator> deserialized = mapper.reader(
        new TypeReference<List<QueryInterpolator>>()
        {
        }).readValue(serialized);
    Assert.assertEquals(expected.size(), deserialized.size());
    for (int i = 0; i < expected.size(); ++i) {
      Assert.assertEquals(expected.get(i), deserialized.get(i));
    }
  }

  @Test
  public void testSerdeQueryProxyBehaviorConfig() throws Exception
  {
    QueryProxyBehaviorConfig config = new QueryProxyBehaviorConfig(ImmutableList.of(
        new QueryIntervalDurationInterpolator(
            ImmutableList.of(),
            ImmutableList.of(),
            7776000000L
        )), 1, 2, 3);
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    String serialized = mapper.writeValueAsString(config);
    QueryProxyBehaviorConfig deserialized = mapper.reader(QueryProxyBehaviorConfig.class).readValue(serialized);
    Assert.assertEquals(config.getQueryInterpolators().get(0), deserialized.getQueryInterpolators().get(0));

    Assert.assertEquals(1, deserialized.getQueryQueueSize());
    Assert.assertEquals(2, deserialized.getStartDelayMillis());
    Assert.assertEquals(3, deserialized.getMaxRunningQueries());
  }
}
