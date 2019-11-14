/*
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
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import static java.util.stream.Collectors.toMap;

import com.google.cloud.datacatalog.DataCatalogGrpc;
import com.google.cloud.datacatalog.DataCatalogGrpc.DataCatalogBlockingStub;
import com.google.cloud.datacatalog.Entry;
import com.google.cloud.datacatalog.LookupEntryRequest;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.auth.MoreCallCredentials;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.FullNameTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubJsonTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;

/** Uses DataCatalog to get the source type and schema for a table. */
public class DataCatalogTableProvider extends FullNameTableProvider {

  private static final TableFactory PUBSUB_TABLE_FACTORY = new PubsubTableFactory();
  private static final TableFactory GCS_TABLE_FACTORY = new GcsTableFactory();

  private static final Map<String, TableProvider> DELEGATE_PROVIDERS =
      Stream.of(new PubsubJsonTableProvider(), new BigQueryTableProvider(), new TextTableProvider())
          .collect(toMap(TableProvider::getTableType, p -> p));

  private final DataCatalogBlockingStub dataCatalog;
  private final Map<String, Table> tableCache;
  private final TableFactory tableFactory;

  private DataCatalogTableProvider(
      DataCatalogBlockingStub dataCatalog, boolean truncateTimestamps) {

    this.tableCache = new HashMap<>();
    this.dataCatalog = dataCatalog;
    this.tableFactory =
        ChainedTableFactory.of(
            PUBSUB_TABLE_FACTORY, GCS_TABLE_FACTORY, new BigQueryTableFactory(truncateTimestamps));
  }

  public static DataCatalogTableProvider create(DataCatalogPipelineOptions options) {
    return new DataCatalogTableProvider(
        createDataCatalogClient(options), options.getTruncateTimestamps());
  }

  @Override
  public String getTableType() {
    return "google.cloud.datacatalog";
  }

  @Override
  public void createTable(Table table) {
    throw new UnsupportedOperationException(
        "Creating tables is not supported with DataCatalog table provider.");
  }

  @Override
  public void dropTable(String tableName) {
    throw new UnsupportedOperationException(
        "Dropping tables is not supported with DataCatalog table provider");
  }

  @Override
  public Map<String, Table> getTables() {
    throw new UnsupportedOperationException("Loading all tables from DataCatalog is not supported");
  }

  @Override
  public @Nullable Table getTable(String tableName) {
    return loadTable(tableName);
  }

  @Override
  public @Nullable Table getTableByFullName(TableName fullTableName) {

    ImmutableList<String> allNameParts =
        ImmutableList.<String>builder()
            .addAll(fullTableName.getPath())
            .add(fullTableName.getTableName())
            .build();

    String fullEscapedTableName = ZetaSqlIdUtils.escapeAndJoin(allNameParts);

    return loadTable(fullEscapedTableName);
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return DELEGATE_PROVIDERS.get(table.getType()).buildBeamSqlTable(table);
  }

  private @Nullable Table loadTable(String tableName) {
    if (!tableCache.containsKey(tableName)) {
      tableCache.put(tableName, loadTableFromDC(tableName));
    }

    return tableCache.get(tableName);
  }

  private Table loadTableFromDC(String tableName) {
    try {
      return toCalciteTable(
          tableName,
          dataCatalog.lookupEntry(
              LookupEntryRequest.newBuilder().setSqlResource(tableName).build()));
    } catch (StatusRuntimeException e) {
      if (e.getStatus().equals(Status.INVALID_ARGUMENT)) {
        return null;
      }
      throw new RuntimeException(e);
    }
  }

  private static DataCatalogBlockingStub createDataCatalogClient(
      DataCatalogPipelineOptions options) {
    return DataCatalogGrpc.newBlockingStub(
            ManagedChannelBuilder.forTarget(options.getDataCatalogEndpoint()).build())
        .withCallCredentials(
            MoreCallCredentials.from(options.as(GcpOptions.class).getGcpCredential()));
  }

  private Table toCalciteTable(String tableName, Entry entry) {
    if (entry.getSchema().getColumnsCount() == 0) {
      throw new UnsupportedOperationException(
          "Entry doesn't have a schema. Please attach a schema to '"
              + tableName
              + "' in Data Catalog: "
              + entry.toString());
    }
    Schema schema = SchemaUtils.fromDataCatalog(entry.getSchema());

    Optional<Table.Builder> tableBuilder = tableFactory.tableBuilder(entry);
    if (!tableBuilder.isPresent()) {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported Data Catalog entry: %s",
              MoreObjects.toStringHelper(entry)
                  .add("linkedResource", entry.getLinkedResource())
                  .add("hasGcsFilesetSpec", entry.hasGcsFilesetSpec())
                  .toString()));
    }

    return tableBuilder.get().schema(schema).name(tableName).build();
  }
}
