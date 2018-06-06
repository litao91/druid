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

package io.druid.segment.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.DateTimes;
import io.druid.query.expression.TestExprMacroTable;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.TestHelper;
import io.druid.segment.transform.ExpressionTransform;
import io.druid.segment.transform.SeparatorSplit;
import io.druid.segment.transform.TransformSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class TransformSpecTest
{
  private static final MapInputRowParser PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec("t", "auto", DateTimes.of("2000-01-01")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("f", "x", "y")),
              null,
              null
          )
      )
  );

  private static final Map<String, Object> ROW1 = ImmutableMap.<String, Object>builder()
      .put("x", "foo")
      .put("y", "bar")
      .put("a", 2.0)
      .put("b", 3L)
      .build();

  private static final Map<String, Object> ROW2 = ImmutableMap.<String, Object>builder()
      .put("x", "foo")
      .put("y", "baz")
      .put("a", 2.0)
      .put("b", 4L)
      .build();

  private static final Map<String, Object> ROW3 = ImmutableMap.<String, Object>builder()
      .put("x", "foo,bar,hello,world")
      .put("y", "b|ac|zd")
      .put("a", 2.0)
      .put("b", 4L)
      .build();


  @Test
  public void testTransforms()
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("f", "concat(x,y)", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("g", "a + b", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("h", "concat(f,g)", TestExprMacroTable.INSTANCE)
        )
    );

    final InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);
    final InputRow row = parser.parseBatch(ROW1).get(0);

    Assert.assertNotNull(row);
    Assert.assertEquals(DateTimes.of("2000-01-01").getMillis(), row.getTimestampFromEpoch());
    Assert.assertEquals(DateTimes.of("2000-01-01"), row.getTimestamp());
    Assert.assertEquals(ImmutableList.of("f", "x", "y"), row.getDimensions());
    Assert.assertEquals(ImmutableList.of("foo"), row.getDimension("x"));
    Assert.assertEquals(3.0, row.getMetric("b").doubleValue(), 0);
    Assert.assertEquals("foobar", row.getRaw("f"));
    Assert.assertEquals(ImmutableList.of("foobar"), row.getDimension("f"));
    Assert.assertEquals(ImmutableList.of("5.0"), row.getDimension("g"));
    Assert.assertEquals(ImmutableList.of(), row.getDimension("h"));
    Assert.assertEquals(5L, row.getMetric("g").longValue());
  }

  @Test
  public void testTransformOverwriteField()
  {
    // Transforms are allowed to overwrite fields, and to refer to the fields they overwrite; double-check this.

    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("x", "concat(x,y)", TestExprMacroTable.INSTANCE)
        )
    );

    final InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);
    final InputRow row = parser.parseBatch(ROW1).get(0);

    Assert.assertNotNull(row);
    Assert.assertEquals(DateTimes.of("2000-01-01").getMillis(), row.getTimestampFromEpoch());
    Assert.assertEquals(DateTimes.of("2000-01-01"), row.getTimestamp());
    Assert.assertEquals(ImmutableList.of("f", "x", "y"), row.getDimensions());
    Assert.assertEquals(ImmutableList.of("foobar"), row.getDimension("x"));
    Assert.assertEquals(3.0, row.getMetric("b").doubleValue(), 0);
    Assert.assertNull(row.getRaw("f"));
  }

  @Test
  public void testFilterOnTransforms()
  {
    // Filters are allowed to refer to transformed fields; double-check this.

    final TransformSpec transformSpec = new TransformSpec(
        new AndDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("x", "foo", null),
                new SelectorDimFilter("f", "foobar", null),
                new SelectorDimFilter("g", "5.0", null)
            )
        ),
        ImmutableList.of(
            new ExpressionTransform("f", "concat(x,y)", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("g", "a + b", TestExprMacroTable.INSTANCE)
        )
    );

    final InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);
    Assert.assertNotNull(parser.parseBatch(ROW1).get(0));
    Assert.assertNull(parser.parseBatch(ROW2).get(0));
  }

  @Test
  public void testTransformTimeFromOtherFields()
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("__time", "(a + b) * 3600000", TestExprMacroTable.INSTANCE)
        )
    );

    final InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);
    final InputRow row = parser.parseBatch(ROW1).get(0);

    Assert.assertNotNull(row);
    Assert.assertEquals(DateTimes.of("1970-01-01T05:00:00Z"), row.getTimestamp());
    Assert.assertEquals(DateTimes.of("1970-01-01T05:00:00Z").getMillis(), row.getTimestampFromEpoch());
  }

  @Test
  public void testTransformTimeFromTime()
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("__time", "__time + 3600000", TestExprMacroTable.INSTANCE)
        )
    );

    final InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);
    final InputRow row = parser.parseBatch(ROW1).get(0);

    Assert.assertNotNull(row);
    Assert.assertEquals(DateTimes.of("2000-01-01T01:00:00Z"), row.getTimestamp());
    Assert.assertEquals(DateTimes.of("2000-01-01T01:00:00Z").getMillis(), row.getTimestampFromEpoch());
  }

  @Test
  public void testSeparatorSplitter()
  {
    final TransformSpec transformSpec = new TransformSpec(
        ImmutableList.of(
            new SeparatorSplit("x", ","),
            new SeparatorSplit("y", "\\|")
        ),
        null,
        null
    );
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);
    final List<InputRow> rows = parser.parseBatch(ROW3);
    for (InputRow r : rows) {
      System.out.println("x: " + r.getRaw("x") + ", y:" + r.getRaw("y"));
    }
    Assert.assertEquals(12, rows.size());
    Assert.assertEquals("foo", rows.get(0).getRaw("x"));
    Assert.assertEquals("b", rows.get(0).getRaw("y"));
    Assert.assertEquals("foo", rows.get(1).getRaw("x"));
    Assert.assertEquals("ac", rows.get(1).getRaw("y"));
    Assert.assertEquals("foo", rows.get(2).getRaw("x"));
    Assert.assertEquals("zd", rows.get(2).getRaw("y"));
    Assert.assertEquals("bar", rows.get(3).getRaw("x"));
    Assert.assertEquals("world", rows.get(11).getRaw("x"));
    Assert.assertEquals("zd", rows.get(11).getRaw("y"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final TransformSpec transformSpec = new TransformSpec(
        ImmutableList.of(
            new SeparatorSplit("test", ","),
            new SeparatorSplit("test2", "|")
        ),
        new AndDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("x", "foo", null),
                new SelectorDimFilter("f", "foobar", null),
                new SelectorDimFilter("g", "5.0", null)
            )
        ),
        ImmutableList.of(
            new ExpressionTransform("f", "concat(x,y)", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("g", "a + b", TestExprMacroTable.INSTANCE)
        )
    );


    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    String deserialized = jsonMapper.writeValueAsString(transformSpec);

    Assert.assertEquals(
        transformSpec,
        jsonMapper.readValue(deserialized, TransformSpec.class)
    );
  }

  @Test
  public void fullParsingTest() throws Exception
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    String datasourceConfig = readFileFromClasspathAsString("test_datasource.json");
    DataSchema schema = mapper.readValue(datasourceConfig, DataSchema.class);
    InputRowParser parser = schema.getParser();
    String data1 = readFileFromClasspathAsString("testdata.json");
    List<InputRow> rows1 = parser.parseBatch(ByteBuffer.wrap(data1.getBytes("UTF-8")));
    Assert.assertEquals(1, rows1.size());

    String data2 = readFileFromClasspathAsString("testdata2.json");
    List<InputRow> rows2 = parser.parseBatch(ByteBuffer.wrap(data2.getBytes("UTF-8")));
    Assert.assertEquals(1, rows2.size());

    String data3 = readFileFromClasspathAsString("testdata3.json");
    List<InputRow> rows3 = parser.parseBatch(ByteBuffer.wrap(data3.getBytes("UTF-8")));
    Assert.assertEquals(24, rows3.size());
  }

  public final static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(TransformSpecTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
