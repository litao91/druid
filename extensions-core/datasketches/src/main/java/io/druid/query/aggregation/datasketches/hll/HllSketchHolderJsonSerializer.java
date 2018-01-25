package io.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class HllSketchHolderJsonSerializer extends JsonSerializer<HllSketchHolder> {
  @Override
  public void serialize(HllSketchHolder hllSketchHolder, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
    jsonGenerator.writeBinary(hllSketchHolder.getHllSketch().toCompactByteArray());
  }
}
