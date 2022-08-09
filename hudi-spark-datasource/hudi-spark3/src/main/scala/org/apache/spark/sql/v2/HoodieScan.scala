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

import org.apache.avro.Schema
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.HoodieBaseRelation.{convertToAvroSchema, getPartitionPath}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieSparkUtils.sparkAdapter
import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, HoodieDataSourceHelper, HoodieFileIndex}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.internal.schema.{HoodieSchemaException, InternalSchema}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.execution.datasources.HoodieInMemoryFileIndex
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetPartitionReaderFactory
import org.apache.spark.sql.execution.datasources.{FileStatusCache, PartitioningUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import scala.collection.JavaConverters.{asScalaIteratorConverter, mapAsScalaMapConverter}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 */
case class HoodieScan(sparkSession: SparkSession,
                      table: HoodieCatalogTable,
                      schemaSpec: Option[StructType],
                      optParams: Map[String, String],
                      partitionFilters: Seq[Expression],
                      dataFilters: Seq[Expression])
  extends Scan with Batch with Logging {

  def metaClient: HoodieTableMetaClient = table.metaClient

  protected lazy val tableConfig: HoodieTableConfig = metaClient.getTableConfig

  protected def timeline: HoodieTimeline =
  // NOTE: We're including compaction here since it's not considering a "commit" operation
    metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants

  /**
   * NOTE: Initialization of teh following members is coupled on purpose to minimize amount of I/O
   *       required to fetch table's Avro and Internal schemas
   */
  protected lazy val (tableAvroSchema: Schema, internalSchema: InternalSchema) = {
    val schemaResolver = new TableSchemaResolver(metaClient)
    val avroSchema: Schema = schemaSpec.map(convertToAvroSchema).getOrElse {
      Try(schemaResolver.getTableAvroSchema) match {
        case Success(schema) => schema
        case Failure(e) =>
          logError("Failed to fetch schema from the table", e)
          throw new HoodieSchemaException("Failed to fetch schema from the table")
      }
    }

    val internalSchema: InternalSchema = if (!isSchemaEvolutionEnabled) {
      InternalSchema.getEmptyInternalSchema
    } else {
      Try(schemaResolver.getTableInternalSchemaFromCommitMetadata) match {
        case Success(internalSchemaOpt) =>
          toScalaOption(internalSchemaOpt).getOrElse(InternalSchema.getEmptyInternalSchema)
        case Failure(e) =>
          logWarning("Failed to fetch internal-schema from the table", e)
          InternalSchema.getEmptyInternalSchema
      }
    }

    (avroSchema, internalSchema)
  }

  protected lazy val tableStructSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)

  protected val partitionColumns: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

  /**
   * NOTE: PLEASE READ THIS CAREFULLY
   *
   * Even though [[HoodieFileIndex]] initializes eagerly listing all of the files w/in the given Hudi table,
   * this variable itself is _lazy_ (and have to stay that way) which guarantees that it's not initialized, until
   * it's actually accessed
   */
  protected lazy val fileIndex: HoodieFileIndex =
    HoodieFileIndex(sparkSession, metaClient, Some(tableStructSchema), optParams,
      FileStatusCache.getOrCreate(sparkSession))

  // todo did diff data schema can use?
  override def readSchema(): StructType = table.tableSchema

  override def planInputPartitions(): Array[InputPartition] = {
    val partitions = listLatestBaseFiles(Seq.empty, partitionFilters, dataFilters)
    val fileSplits = partitions.values.toSeq
      .flatMap { files =>
        files.flatMap { file =>
          // TODO fix, currently assuming parquet as underlying format
          HoodieDataSourceHelper.splitFiles(
            sparkSession = sparkSession,
            file = file,
            partitionValues = getPartitionColumnsAsInternalRow(file)
          )
        }
      }
      // NOTE: It's important to order the splits in the reverse order of their
      //       size so that we can subsequently bucket them in an efficient manner
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val tableBroadCast = sparkSession.sparkContext.broadcast(new SerializableTable(table.tableSchema, table.dataSchema))
    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    sparkAdapter.getFilePartitions(sparkSession, fileSplits, maxSplitBytes)
      .map(filePartition => HoodieFileSplitPartition(filePartition, tableBroadCast)).toArray
  }

  override def toBatch: Batch = this

  override def createReaderFactory(): PartitionReaderFactory = {
    HoodiePartitionReaderFactory(createParquetReaderFactory().asInstanceOf[ParquetPartitionReaderFactory],
      Seq(table.metaClient.getBasePathV2.toString), Option(tableStructSchema))
  }

  def createParquetReaderFactory(): PartitionReaderFactory = {
    val hadoopConf =
      sparkSession.sessionState.newHadoopConfWithOptions(CaseInsensitiveStringMap.empty().asCaseSensitiveMap.asScala.toMap)
    val readDataSchemaAsJson = table.dataSchema.json
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    ParquetWriteSupport.setSchema(table.dataSchema, hadoopConf)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)

    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val sqlConf = sparkSession.sessionState.conf
    ParquetPartitionReaderFactory(
      sqlConf,
      broadcastedConf,
      table.dataSchema,
      table.dataSchema,
      table.partitionSchema,
      Array.empty,
      new ParquetOptions(CaseInsensitiveStringMap.empty().asCaseSensitiveMap.asScala.toMap, sqlConf))
  }

  protected def listLatestBaseFiles(globbedPaths: Seq[Path], partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Map[Path, Seq[FileStatus]] = {
    val partitionDirs = if (globbedPaths.isEmpty) {
      fileIndex.listFiles(partitionFilters, dataFilters)
    } else {
      val inMemoryFileIndex = HoodieInMemoryFileIndex.create(sparkSession, globbedPaths)
      inMemoryFileIndex.listFiles(partitionFilters, dataFilters)
    }

    val fsView = new HoodieTableFileSystemView(metaClient, timeline, partitionDirs.flatMap(_.files).toArray)
    val latestBaseFiles = fsView.getLatestBaseFiles.iterator().asScala.toList.map(_.getFileStatus)

    latestBaseFiles.groupBy(getPartitionPath)
  }

  /**
   * Controls whether partition values (ie values of partition columns) should be
   * <ol>
   *    <li>Extracted from partition path and appended to individual rows read from the data file (we
   *    delegate this to Spark's [[ParquetFileFormat]])</li>
   *    <li>Read from the data-file as is (by default Hudi persists all columns including partition ones)</li>
   * </ol>
   *
   * This flag is only be relevant in conjunction with the usage of [["hoodie.datasource.write.drop.partition.columns"]]
   * config, when Hudi will NOT be persisting partition columns in the data file, and therefore values for
   * such partition columns (ie "partition values") will have to be parsed from the partition path, and appended
   * to every row only in the fetched dataset.
   *
   * NOTE: Partition values extracted from partition path might be deviating from the values of the original
   *       partition columns: for ex, if originally as partition column was used column [[ts]] bearing epoch
   *       timestamp, which was used by [[TimestampBasedKeyGenerator]] to generate partition path of the format
   *       [["yyyy/mm/dd"]], appended partition value would bear the format verbatim as it was used in the
   *       partition path, meaning that string value of "2022/01/01" will be appended, and not its original
   *       representation
   */
  protected val shouldExtractPartitionValuesFromPartitionPath: Boolean = {
    // Controls whether partition columns (which are the source for the partition path values) should
    // be omitted from persistence in the data files. On the read path it affects whether partition values (values
    // of partition columns) will be read from the data file or extracted from partition path
    val shouldOmitPartitionColumns = metaClient.getTableConfig.shouldDropPartitionColumns && partitionColumns.nonEmpty
    val shouldExtractPartitionValueFromPath =
      optParams.getOrElse(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key,
        DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.defaultValue.toString).toBoolean
    shouldOmitPartitionColumns || shouldExtractPartitionValueFromPath
  }

  /**
   * For enable hoodie.datasource.write.drop.partition.columns, need to create an InternalRow on partition values
   * and pass this reader on parquet file. So that, we can query the partition columns.
   */
  protected def getPartitionColumnsAsInternalRow(file: FileStatus): InternalRow = {
    try {
      val tableConfig = metaClient.getTableConfig
      if (shouldExtractPartitionValuesFromPartitionPath) {
        val relativePath = new URI(metaClient.getBasePath).relativize(new URI(file.getPath.getParent.toString)).toString
        val hiveStylePartitioningEnabled = tableConfig.getHiveStylePartitioningEnable.toBoolean
        if (hiveStylePartitioningEnabled) {
          val partitionSpec = PartitioningUtils.parsePathFragment(relativePath)
          InternalRow.fromSeq(partitionColumns.map(partitionSpec(_)).map(UTF8String.fromString))
        } else {
          if (partitionColumns.length == 1) {
            InternalRow.fromSeq(Seq(UTF8String.fromString(relativePath)))
          } else {
            val parts = relativePath.split("/")
            assert(parts.size == partitionColumns.length)
            InternalRow.fromSeq(parts.map(UTF8String.fromString))
          }
        }
      } else {
        InternalRow.empty
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get the right partition InternalRow for file: ${file.toString}", e)
        InternalRow.empty
    }
  }

  private def isSchemaEvolutionEnabled = {
    // NOTE: Schema evolution could be configured both t/h optional parameters vehicle as well as
    //       t/h Spark Session configuration (for ex, for Spark SQL)
    optParams.getOrElse(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key,
      DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue.toString).toBoolean ||
      sparkSession.conf.get(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key,
        DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue.toString).toBoolean
  }
}
