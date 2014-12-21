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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroDeserializer implements EventDeserializer<GenericRecord> {

  static Configuration conf = new Configuration();
  private static LoadingCache<String, Schema> schemasFromLiteral = CacheBuilder
      .newBuilder()
      .build(new CacheLoader<String, Schema>() {
        @Override
        public Schema load(String literal) {
          Preconditions.checkNotNull(literal,
              "Schema literal cannot be null without a Schema URL");
          return new Schema.Parser().parse(literal);
        }
      });
  private static LoadingCache<String, Schema> schemasFromURL = CacheBuilder
      .newBuilder()
      .build(new CacheLoader<String, Schema>() {
        @Override
        public Schema load(String url) throws IOException {
          Schema.Parser parser = new Schema.Parser();
          InputStream is = null;
          try {
            FileSystem fs = FileSystem.get(URI.create(url), conf);
            if (url.toLowerCase(Locale.ENGLISH).startsWith("hdfs:/")) {
              is = fs.open(new Path(url));
            } else {
              is = new URL(url).openStream();
            }
            return parser.parse(is);
          } finally {
            if (is != null) {
              is.close();
            }
          }
        }
      });

  private final Schema targetSchema;
  private LoadingCache<Schema, DatumReader<GenericRecord>> readers =
      CacheBuilder.newBuilder()
          .build(new CacheLoader<Schema, DatumReader<GenericRecord>>() {
            @Override
            public DatumReader<GenericRecord> load(Schema schema) {
              // must use the target dataset's schema for reading to ensure the
              // records are able to be stored using it
              return new GenericDatumReader<GenericRecord>(
                  schema, targetSchema);
            }
          });

  private BinaryDecoder decoder = null;

  public AvroDeserializer(Schema targetSchema) {
    this.targetSchema = targetSchema;
  }

  @Override
  public GenericRecord deserialize(Event event, GenericRecord reuse) throws EventDeliveryException {
    decoder = DecoderFactory.get().binaryDecoder(event.getBody(), decoder);
    // no checked exception is thrown in the CacheLoader
    DatumReader<GenericRecord> reader = readers.getUnchecked(schema(event));
    try {
      return reader.read(reuse, decoder);
    } catch (IOException ex) {
      throw new EventDeliveryException("Cannot deserialize event", ex);
    }
  }

  private static Schema schema(Event event) throws EventDeliveryException {
    Map<String, String> headers = event.getHeaders();
    String schemaURL = headers.get(
        DatasetSinkConstants.AVRO_SCHEMA_URL_HEADER);
    try {
      if (headers.get(DatasetSinkConstants.AVRO_SCHEMA_URL_HEADER) != null) {
        return schemasFromURL.get(schemaURL);
      } else {
        return schemasFromLiteral.get(
            headers.get(DatasetSinkConstants.AVRO_SCHEMA_LITERAL_HEADER));
      }
    } catch (ExecutionException ex) {
      throw new EventDeliveryException("Cannot get schema", ex.getCause());
    }
  }
}
