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
package io.druid.server.log;

import com.google.common.base.Supplier;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.metadata.MetadataStorageTablesConfig; 

import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

@JsonTypeName("metadata")
public class SQLMetadataQueryRequestLoggerProvider implements RequestLoggerProvider
{
  private static final Logger log = new Logger(SQLMetadataQueryRequestLoggerProvider.class);
  private SQLMetadataConnector connector;
  private String queryLogTableName;

  @JsonProperty
  private String queryTypeWhiteList = null;

  @JacksonInject
  @NotNull
  @Json
  private ObjectMapper jsonMapper = null;

  @Inject
  public void injectMe(SQLMetadataConnector connector, Supplier<MetadataStorageTablesConfig> dbTables)
  {
    this.connector = connector;
    this.queryLogTableName = dbTables.get().getQueryLogTable();
  }

  @Override
  public RequestLogger get()
  {
    Set<String> whitelist;
    if (queryTypeWhiteList == null) {
      whitelist = null;
    } else {
      whitelist = new HashSet<String>(Arrays.asList(queryTypeWhiteList.split(",")));
    }
    SQLMetadataQueryRequestLogger logger = new SQLMetadataQueryRequestLogger(connector, queryLogTableName, whitelist, jsonMapper);
    log.debug(new Exception("stack trace"), "Creating %s at", logger);
    return logger;
  }
}
