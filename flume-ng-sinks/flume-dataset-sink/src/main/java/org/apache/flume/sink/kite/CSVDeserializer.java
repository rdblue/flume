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

package org.apache.flume.sink.kite;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVRecordParser;

import static org.apache.flume.sink.kite.DatasetSinkConstants.CONFIG_CSV_DELIMITER;
import static org.apache.flume.sink.kite.DatasetSinkConstants.CONFIG_CSV_ESCAPE_CHAR;
import static org.apache.flume.sink.kite.DatasetSinkConstants.CONFIG_CSV_HEADER;
import static org.apache.flume.sink.kite.DatasetSinkConstants.CONFIG_CSV_CHARSET;
import static org.apache.flume.sink.kite.DatasetSinkConstants.CONFIG_CSV_QUOTE_CHAR;

public class CSVDeserializer implements EventDeserializer<GenericRecord> {

  private static final Splitter HEADER_SPLITTER = Splitter.on(',');

  private LoadingCache<String, CSVRecordParser<GenericRecord>> parsers =
      CacheBuilder.newBuilder()
          .build(new CacheLoader<String, CSVRecordParser<GenericRecord>>() {
            @Override
            public CSVRecordParser<GenericRecord> load(@Nonnull String header) {
              return new CSVRecordParser<GenericRecord>(
                  props, view, parseHeader(header));
            }
          });

  private final CSVProperties props;
  private final CharsetDecoder decoder;
  private final View<GenericRecord> view;
  private final CSVRecordParser<GenericRecord> defaultParser;

  public CSVDeserializer(Context config, View<GenericRecord> view) {
    this.props = fromContext(config);
    this.decoder = Charset.forName(props.charset).newDecoder();
    this.view = view;
    this.defaultParser = new CSVRecordParser<GenericRecord>(props, view,
        parseHeader(config.getString(CONFIG_CSV_HEADER)));
  }

  @Override
  public GenericRecord deserialize(Event event, GenericRecord reuse)
      throws EventDeliveryException {
    CSVRecordParser<GenericRecord> parser = defaultParser;

    // if the event has its own header, get a parser for it
    if (event.getHeaders().containsKey(CONFIG_CSV_HEADER)) {
      try {
        parser = parsers.get(event.getHeaders().get(CONFIG_CSV_HEADER));
      } catch (ExecutionException ex) {
        throw new EventDeliveryException("Cannot get schema", ex.getCause());
      }
    }

    try {
      return parser.read(decode(event.getBody(), decoder), reuse);
    } catch (CharacterCodingException e) {
      throw new EventDeliveryException("Cannot decode body", e);
    }
  }

  private static String decode(byte[] bytes, CharsetDecoder decoder)
      throws CharacterCodingException {
    // copies the bytes to a String, which is unavoidable because the parser
    // requires a String and not a CharSequence
    return decoder.decode(ByteBuffer.wrap(bytes)).toString();
  }

  private static CSVProperties fromContext(Context config) {
    CSVProperties.Builder builder = new CSVProperties.Builder();
    if (config.containsKey(CONFIG_CSV_CHARSET)) {
      builder.charset(config.getString(CONFIG_CSV_CHARSET));
    }
    if (config.containsKey(CONFIG_CSV_DELIMITER)) {
      builder.delimiter(config.getString(CONFIG_CSV_DELIMITER));
    }
    if (config.containsKey(CONFIG_CSV_QUOTE_CHAR)) {
      builder.quote(config.getString(CONFIG_CSV_QUOTE_CHAR));
    }
    if (config.containsKey(CONFIG_CSV_ESCAPE_CHAR)) {
      builder.escape(config.getString(CONFIG_CSV_ESCAPE_CHAR));
    }
    return builder.build();
  }

  private static List<String> parseHeader(@Nullable String headerString) {
    if (headerString == null) {
      return null;
    }

    List<String> fields = Lists.newArrayList();
    for (String field : HEADER_SPLITTER.split(headerString)) {
      fields.add(field.trim());
    }

    return fields;
  }
}
