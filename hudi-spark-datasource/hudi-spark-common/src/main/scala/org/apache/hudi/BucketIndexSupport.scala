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

package org.apache.hudi

import org.apache.hadoop.fs.FileStatus
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.bucket.BucketIdentifier
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.metadata.{HoodieTableMetadata, HoodieTableMetadataUtil}
import org.apache.hudi.util.JFunction
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, In, Literal}

import java.util
import scala.collection.{JavaConverters, mutable}

class BucketIndexSupport(spark: SparkSession,
                         metadataConfig: HoodieMetadataConfig,
                         metaClient: HoodieTableMetaClient) {

  /**
   * Returns the list of candidate files which store the provided record keys based on Metadata Table Record Index.
   * @param allFiles - List of all files which needs to be considered for the query
   * @param recordKeys - List of record keys.
   * @return Sequence of file names which need to be queried
   */
  def getCandidateFiles(allFiles: Seq[FileStatus], bucketIds: Set[String]): Set[String] = {
    val candidateFiles: mutable.Set[String] = mutable.Set.empty
    for (file <- allFiles) {
      val fileId = FSUtils.getFileIdFromFilePath(file.getPath)
      val fileBucketId = BucketIdentifier.bucketIdStrFromFileId(fileId)
      if (bucketIds.contains(fileBucketId)) {
        candidateFiles += file.getPath.getName
      }
    }
    candidateFiles.toSet
  }

  /**
   * Returns the configured record key for the table if it is a simple record key else returns empty option.
   */
  private def getBucketHashField: Option[String] = {
    val bucketHashFields = metadataConfig.getString(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD)
    if (bucketHashFields == null) {
      val recordKeysOpt: org.apache.hudi.common.util.Option[Array[String]] = metaClient.getTableConfig.getRecordKeyFields
      val recordKeyOpt = recordKeysOpt.map[String](JFunction.toJavaFunction[Array[String], String](arr =>
        if (arr.length == 1) {
          arr(0)
        } else {
          null
        }))
      Option.apply(recordKeyOpt.orElse(null))
    } else {
      val fileds = bucketHashFields.split(",")
      if (fileds.length > 1) {
        Option.apply(null)
      } else {
        Option.apply(fileds(0))
      }
    }
  }

  /**
   * Matches the configured simple record key with the input attribute name.
   * @param attributeName The attribute name provided in the query
   * @return true if input attribute name matches the configured simple record key
   */
  private def attributeMatchesBucketHashField(attributeName: String): Boolean = {
    val bucketHashFieldOpt = getBucketHashField
    if (bucketHashFieldOpt.isDefined && bucketHashFieldOpt.get == attributeName) {
      true
    } else {
      false
    }
  }

  /**
   * Returns the attribute and literal pair given the operands of a binary operator. The pair is returned only if one of
   * the operand is an attribute and other is literal. In other cases it returns an empty Option.
   * @param expression1 - Left operand of the binary operator
   * @param expression2 - Right operand of the binary operator
   * @return Attribute and literal pair
   */
  private def getAttributeLiteralTuple(expression1: Expression, expression2: Expression): Option[(AttributeReference, Literal)] = {
    expression1 match {
      case attr: AttributeReference => expression2 match {
        case literal: Literal =>
          Option.apply(attr, literal)
        case _ =>
          Option.empty
      }
      case literal: Literal => expression2 match {
        case attr: AttributeReference =>
          Option.apply(attr, literal)
        case _ =>
          Option.empty
      }
      case _ => Option.empty
    }

  }

  /**
   * Given query filters, it filters the EqualTo and IN queries on simple record key columns and returns a tuple of
   * list of such queries and list of record key literals present in the query.
   * If record index is not available, it returns empty list for record filters and record keys
   * @param queryFilters The queries that need to be filtered.
   * @return Tuple of List of filtered queries and list of record key literals that need to be matched
   */
  def filterQueriesWithBucketHashField(queryFilters: Seq[Expression]): (List[Expression], Set[String]) = {
    val bucketNumber = metadataConfig.getIntOrDefault(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS)
    if (!isIndexAvailable || bucketNumber == null) {
      (List.empty, Set.empty)
    } else {
      var recordKeyQueries: List[Expression] = List.empty
      var bucketValues: List[String] = List.empty
      for (query <- queryFilters) {
        filterQueryWithBucketHashField(query).foreach({
          case (exp: Expression, bValues: List[String]) =>
            bucketValues = bucketValues ++ bValues
            recordKeyQueries = recordKeyQueries :+ exp
        })
      }

      var bucketIds: Set[String] = Set.empty
      for (value <- bucketValues) {
        val bucketId = BucketIdentifier.getBucketId(JavaConverters.seqAsJavaListConverter(List.apply(value)).asJava, bucketNumber)
        bucketIds += BucketIdentifier.bucketIdStr(bucketId)
      }
      Tuple2.apply(recordKeyQueries, bucketIds)
    }
  }

  /**
   * If the input query is an EqualTo or IN query on simple record key columns, the function returns a tuple of
   * list of the query and list of record key literals present in the query otherwise returns an empty option.
   *
   * @param queryFilter The query that need to be filtered.
   * @return Tuple of filtered query and list of record key literals that need to be matched
   */
  private def filterQueryWithBucketHashField(queryFilter: Expression): Option[(Expression, List[String])] = {
    queryFilter match {
      case equalToQuery: EqualTo =>
        val (attribute, literal) = getAttributeLiteralTuple(equalToQuery.left, equalToQuery.right).orNull
        if (attribute != null && attribute.name != null && attributeMatchesBucketHashField(attribute.name)) {
          Option.apply(equalToQuery, List.apply(literal.value.toString))
        } else {
          Option.empty
        }
      case inQuery: In =>
        var validINQuery = true
        inQuery.value match {
          case _: AttributeReference =>
          case _ => validINQuery = false
        }
        var literals: List[String] = List.empty
        inQuery.list.foreach {
          case literal: Literal => literals = literals :+ literal.value.toString
          case _ => validINQuery = false
        }
        if (validINQuery) {
          Option.apply(inQuery, literals)
        } else {
          Option.empty
        }
      case _ => Option.empty
    }
  }

  /**
   * Return true if metadata table is enabled and record index metadata partition is available.
   */
  def isIndexAvailable: Boolean = {
    metadataConfig.enabled && metadataConfig.getBooleanOrDefault("hoodie.bucket.query.index", false)
  }
}

