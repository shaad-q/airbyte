/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.dynamodb;

import com.fasterxml.jackson.databind.JsonNode;

public class DynamodbDestinationConfig {

  private final String endpoint;
  private final String tableNamePrefix;
  private final String accessKeyId;
  private final String secretAccessKey;
  private final String region;
  private final Boolean enableSortKeyFlag;
  private final Boolean overrideSKFlag;
  private final String overrideSK;
  private final Boolean overridePKFlag;
  private final String overridePK;

  public DynamodbDestinationConfig(
                                   final String endpoint,
                                   final String tableNamePrefix,
                                   final String region,
                                   final String accessKeyId,
                                   final String secretAccessKey,
                                   final Boolean enableSortKeyFlag,
                                   final Boolean overrideSKFlag,
                                   final String overrideSK,
                                   final Boolean overridePKFlag,
                                   final String overridePK) {
    this.endpoint = endpoint;
    this.tableNamePrefix = tableNamePrefix;
    this.region = region;
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.enableSortKeyFlag = enableSortKeyFlag;
    this.overrideSKFlag = overrideSKFlag;
    this.overrideSK = overrideSK;
    this.overridePKFlag = overridePKFlag;
    this.overridePK = overridePK;
  }

  public static DynamodbDestinationConfig getDynamodbDestinationConfig(final JsonNode config) {
    return new DynamodbDestinationConfig(
        config.get("dynamodb_endpoint") == null ? "" : config.get("dynamodb_endpoint").asText(),
        config.get("dynamodb_table_name_prefix") == null ? "" : config.get("dynamodb_table_name_prefix").asText(),
        config.get("dynamodb_region").asText(),
        config.get("access_key_id").asText(),
        config.get("secret_access_key").asText(),
        config.get("enable_sort_key").asBoolean(),
        config.get("override_sort_key_flag").asBoolean(),
        config.get("override_sort_key_flag").asBoolean() && config.get("override_sort_key") != null ? config.get("override_sort_key").asText() : "updated_at",
        config.get("override_partition_key_flag").asBoolean(),
        config.get("override_partition_key_flag").asBoolean() && config.get("override_partition_key") != null ? config.get("override_partition_key").asText() : "uuid");
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  public String getRegion() {
    return region;
  }

  public String getTableNamePrefix() {
    return tableNamePrefix;
  }

  public Boolean getOverridePKFlag() {return overridePKFlag;}

  public Boolean getEnableSortKeyFlag() {return enableSortKeyFlag;}

  public Boolean getOverrideSKFlag() {return overrideSKFlag;}

  public String getOverrideSK() {return overrideSK;}

  public String getOverridePK() {return overridePK;}

}
