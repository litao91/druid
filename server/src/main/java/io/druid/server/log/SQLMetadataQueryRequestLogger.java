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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.server.RequestLogLine;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.Set;

public class SQLMetadataQueryRequestLogger implements RequestLogger
{
  private final SQLMetadataConnector connector;
  private final String tableName;
  private final DBI dbi;
  private final ObjectMapper objectMapper;
  private final Set<String> queryTypeWhiteList;

  public SQLMetadataQueryRequestLogger(SQLMetadataConnector connector, String tableName,
      Set<String> whitelist, ObjectMapper objectMapper)
  {
    this.connector = connector;
    this.dbi = connector.getDBI();
    this.objectMapper = objectMapper;
    this.queryTypeWhiteList = whitelist;
    this.tableName = tableName;
  }

  @LifecycleStart
  public void start()
  {
    connector.createQueryLogTable();
  }

  @Override
  public void log(RequestLogLine requestLogLine) throws IOException
  {
    if (queryTypeWhiteList != null && !queryTypeWhiteList.contains(requestLogLine.getQuery().getType())) {
      return;
    }
    dbi.withHandle((HandleCallback<Void>) handle -> {
      handle.createStatement(StringUtils.format(
          "INSERT INTO %s (" +
              "query_id, query_date, remote_addr, datasource, query_type, query_stats, query) " +
              "VALUES " +
              "(:query_id, :query_date, :remote_addr, :datasource, :query_type, :query_stats, :query)",
          tableName))
          .bind("query_id", requestLogLine.getQuery().getId())
          .bind("query_date", requestLogLine.getTimestamp().toDate())
          .bind("remote_addr", requestLogLine.getRemoteAddr())
          .bind("datasource", requestLogLine.getQuery().getDataSource().getNames().get(0))
          .bind("query_type", requestLogLine.getQuery().getType())
          .bind("query_stats", objectMapper.writeValueAsString(requestLogLine.getQueryStats()))
          .bind("query", objectMapper.writeValueAsString(requestLogLine.getQuery()))
          .execute();
      return null;
    });
  }

  @Override
  public String toString()
  {
    return "SQLMetadataQueryRequestLogger{" +
        "tablename=" + tableName + "}";
  }
}
