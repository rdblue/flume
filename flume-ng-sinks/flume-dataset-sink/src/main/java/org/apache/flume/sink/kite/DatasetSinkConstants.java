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

public class DatasetSinkConstants {
  /**
   * URI of the Kite DatasetRepository.
   */
  public static final String CONFIG_KITE_DATASET_URI = "kite.dataset.uri";

  /**
   * URI of the Kite DatasetRepository.
   */
  public static final String CONFIG_KITE_REPO_URI = "kite.repo.uri";

  /**
   * Name of the Kite Dataset to write into.
   */
  public static final String CONFIG_KITE_DATASET_NAME = "kite.dataset.name";

  /**
   * Number of records to process from the incoming channel per call to process.
   */
  public static final String CONFIG_KITE_BATCH_SIZE = "kite.batchSize";
  public static final long DEFAULT_BATCH_SIZE = 100;

  /**
   * Maximum time to wait before finishing files.
   */
  public static final String CONFIG_KITE_ROLL_INTERVAL = "kite.rollInterval";
  public static final int DEFAULT_ROLL_INTERVAL = 30; // seconds

  /**
   *
   */
  public static final String CONFIG_EVENT_PARSER = "kite.event-parser";
  public static final String AVRO_EVENT_PARSER = "avro";
  public static final String CSV_EVENT_PARSER = "csv";
  public static final String DEFAULT_EVENT_PARSER = AVRO_EVENT_PARSER;

  /**
   * Headers with avro schema information is expected.
   */
  public static final String AVRO_SCHEMA_LITERAL_HEADER =
      "flume.avro.schema.literal";
  public static final String AVRO_SCHEMA_URL_HEADER = "flume.avro.schema.url";

  /**
   * Headers for CSV readers
   */
  public static final String CONFIG_CSV_HEADER = "csv.header";
  public static final String CONFIG_CSV_CHARSET = "csv.charset";
  public static final String CONFIG_CSV_DELIMITER = "csv.delimiter";
  public static final String CONFIG_CSV_QUOTE_CHAR = "csv.quote-char";
  public static final String CONFIG_CSV_ESCAPE_CHAR = "csv.escape-char";

  /**
   * Hadoop authentication settings
   */
  public static final String AUTH_PROXY_USER = "auth.proxyUser";
  public static final String AUTH_PRINCIPAL = "auth.kerberosPrincipal";
  public static final String AUTH_KEYTAB = "auth.kerberosKeytab";
}
