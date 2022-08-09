/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.v2

import org.apache.hudi.DataSourceOptionsHelper
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isUsingHiveCatalog
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

/**
 */
case class HoodieV2Table(spark: SparkSession,
                         path: String,
                         catalogTable: Option[CatalogTable] = None,
                         tableIdentifier: Option[String] = None,
                         options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty())
  extends Table with SupportsRead with ProvidesHoodieConfig {

  // todo exclude the catalogTable dependency
  lazy val hoodieCatalogTable: HoodieCatalogTable = if (catalogTable.isDefined) {
    HoodieCatalogTable(spark, catalogTable.get)
  } else {
    val metaClient: HoodieTableMetaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(SparkSession.active.sessionState.newHadoopConf)
      .build()

    val tableConfig: HoodieTableConfig = metaClient.getTableConfig
    val tableName: String = tableConfig.getTableName

    HoodieCatalogTable(spark, TableIdentifier(tableName))
  }

  private lazy val tableSchema: StructType = hoodieCatalogTable.tableSchema

  override def name(): String = hoodieCatalogTable.table.identifier.unquotedString

  override def schema(): StructType = tableSchema

  override def partitioning(): Array[Transform] = {
    hoodieCatalogTable.partitionFields map { col =>
      new IdentityTransform(new FieldReference(Seq(col)))
    }
  }.toArray

  // todo more
  override def capabilities(): util.Set[TableCapability] = Set(
    BATCH_READ
  ).asJava

  override def properties(): util.Map[String, String] = {
    hoodieCatalogTable.catalogProperties.asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    // NOTE: In cases when Hive Metastore is used as catalog and the table is partitioned, schema in the HMS might contain
    //       Hive-specific partitioning columns created specifically for HMS to handle partitioning appropriately. In that
    //       case  we opt in to not be providing catalog's schema, and instead force Hudi relations to fetch the schema
    //       from the table itself
    val userSchema = if (isUsingHiveCatalog(spark)) {
      None
    } else {
      Option(tableSchema)
    }
    val optParams = DataSourceOptionsHelper.parametersWithReadDefaults(buildHoodieConfig(hoodieCatalogTable))
    HoodieScanBuilder(spark, hoodieCatalogTable, userSchema, optParams)
  }
}
