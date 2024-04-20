/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb;

import io.airbyte.cdk.integrations.debezium.DebeziumIteratorConstants;
import io.airbyte.integrations.source.mongodb.cdc.MongoDbDebeziumConstants;
import io.airbyte.integrations.source.mongodb.cdc.MongoDbDebeziumConstants.Configuration;
import java.time.Duration;

public class MongoConstants {

  public static final String AUTH_SOURCE_CONFIGURATION_KEY = MongoDbDebeziumConstants.Configuration.AUTH_SOURCE_CONFIGURATION_KEY;
  public static final Integer CHECKPOINT_INTERVAL = DebeziumIteratorConstants.SYNC_CHECKPOINT_RECORDS;
  public static final String CHECKPOINT_INTERVAL_CONFIGURATION_KEY = "sync_checkpoint_interval";
  public static final Duration CHECKPOINT_DURATION = DebeziumIteratorConstants.SYNC_CHECKPOINT_DURATION;
  public static final String COLLECTION_STATISTICS_COUNT_KEY = "count";
  public static final String COLLECTION_STATISTICS_STORAGE_SIZE_KEY = "size";
  public static final String CONNECTION_STRING_CONFIGURATION_KEY = MongoDbDebeziumConstants.Configuration.CONNECTION_STRING_CONFIGURATION_KEY;
  public static final String COUNT_KEY = "count";
  public static final String CREDENTIALS_PLACEHOLDER = MongoDbDebeziumConstants.Configuration.CREDENTIALS_PLACEHOLDER;
  public static final String DATABASE_CONFIGURATION_KEY = MongoDbDebeziumConstants.Configuration.DATABASE_CONFIGURATION_KEY;
  public static final String DATABASE_CONFIG_CONFIGURATION_KEY = MongoDbDebeziumConstants.Configuration.DATABASE_CONFIG_CONFIGURATION_KEY;
  public static final String DEFAULT_AUTH_SOURCE = "admin";
  public static final Integer DEFAULT_DISCOVER_SAMPLE_SIZE = 10000;
  public static final String DISCOVER_SAMPLE_SIZE_CONFIGURATION_KEY = "discover_sample_size";
  public static final String DRIVER_NAME = "Airbyte";
  public static final String ID_FIELD = "_id";
  public static final String IS_TEST_CONFIGURATION_KEY = "is_test";
  public static final String PASSWORD_CONFIGURATION_KEY = MongoDbDebeziumConstants.Configuration.PASSWORD_CONFIGURATION_KEY;
  public static final String QUEUE_SIZE_CONFIGURATION_KEY = "queue_size";
  public static final String STORAGE_STATS_KEY = "storageStats";
  public static final String USERNAME_CONFIGURATION_KEY = MongoDbDebeziumConstants.Configuration.USERNAME_CONFIGURATION_KEY;
  public static final String SCHEMA_ENFORCED_CONFIGURATION_KEY = MongoDbDebeziumConstants.Configuration.SCHEMA_ENFORCED_CONFIGURATION_KEY;
  public static final String SCHEMALESS_MODE_DATA_FIELD = Configuration.SCHEMALESS_MODE_DATA_FIELD;
  public static final String INITIAL_RECORD_WAITING_TIME_SEC = "initial_waiting_seconds";
  public static final Integer DEFAULT_INITIAL_RECORD_WAITING_TIME_SEC = 300;

  public static final String INVALID_CDC_CURSOR_POSITION_PROPERTY = "invalid_cdc_cursor_position_behavior";
  public static final String FAIL_SYNC_OPTION = "Fail sync";
  public static final String RESYNC_DATA_OPTION = "Re-sync data";

  public static final String PARAM_CA_CERTIFICATE = "ca_certificate";
  public static final String PARAM_CLIENT_CERTIFICATE = "client_certificate";
  public static final String PARAM_CLIENT_KEY = "client_key";  
  public static final String PARAM_CLIENT_KEY_PASSWORD_IN = "client_key_password_in";
  public static final String PARAM_CLIENT_KEY_PASSWORD_OUT = "client_key_password_out";
  public static final String PARAM_SSL_KEY = "ssl_mode";
  public static final String PARAM_SSL_MODE_KEY = "mode";
  public static final String PARAM_SSL_MODE_CCV = "CCV";

  public static final String CLIENT_CERTIFICATE = "client.crt";
  public static final String CLIENT_CA_CERTIFICATE = "client-ca.crt";
  public static final String CLIENT_KEY = "client.key";
  public static final String CLIENT_KEY_STORE = "client_key_store.p12";
  public static final String KEY_STORE_TYPE = "PKCS12";
  public static final String TRUST_STORE = "truststore.jks";
  public static final String TRUST_PASSWORD = "truststore_pwd";
  public static final String TRUST_TYPE = "JKS";

  private MongoConstants() {}

}
