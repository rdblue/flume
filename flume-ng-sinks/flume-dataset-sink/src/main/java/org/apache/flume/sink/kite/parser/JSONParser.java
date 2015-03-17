/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.kite.parser;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.kitesdk.data.spi.JsonUtil;

import static org.apache.flume.sink.kite.DatasetSinkConstants.CONFIG_JSON_CHARSET;

public class JSONParser implements EntityParser<GenericRecord> {

  private static final GenericData model = GenericData.get();
  private final Schema schema;
  private final CharsetDecoder decoder;

  public JSONParser(Schema schema, Context config) {
    this.schema = schema;
    this.decoder = charset(config).newDecoder();
  }

  @Override
  @SuppressWarnings("unchecked")
  public GenericRecord parse(Event event, GenericRecord reuse)
      throws EventDeliveryException {
    String bodyAsString;
    try {
      bodyAsString = decode(event.getBody(), decoder);
    } catch (CharacterCodingException e) {
      throw new EventDeliveryException("Cannot decode body", e);
    }

    JsonNode datum = JsonUtil.parse(bodyAsString, JsonNode.class);

    return (GenericRecord) JsonUtil.convertToAvro(model, datum, schema);
  }

  private static String decode(byte[] bytes, CharsetDecoder decoder)
      throws CharacterCodingException {
    // copies the bytes to a String, which is unavoidable because the parser
    // requires a String and not a CharSequence
    return decoder.decode(ByteBuffer.wrap(bytes)).toString();
  }

  private static Charset charset(Context config) {
    return Charset.forName(config.getString(CONFIG_JSON_CHARSET, "utf8"));
  }

  public static class Builder implements EntityParser.Builder<GenericRecord> {
    @Override
    public EntityParser<GenericRecord> build(Schema datasetSchema, Context config) {
      return new JSONParser(datasetSchema, config);
    }
  }
}
