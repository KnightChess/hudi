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

import org.apache.hudi.HoodieSparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 */
case class HoodieScanBuilder(sparkSession: SparkSession,
                             table: HoodieCatalogTable,
                             schemaSpec: Option[StructType],
                             optParams: Map[String, String]) extends ScanBuilder with SupportsPushDownFilters  with Logging {

  private var filters: Array[Filter] = Array.empty

  private var partitionFilters: Seq[Expression] = _

  private var dataFilters: Seq[Expression] = _

  override def build(): Scan = {
    HoodieScan(sparkSession, table, schemaSpec, optParams, partitionFilters, dataFilters)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    this.filters = filters
    val filterExpressions = convertToExpressions(filters)
    val (partitionFilters, dataFilters) = filterExpressions.partition(isPartitionPredicate)
    this.partitionFilters = partitionFilters
    this.dataFilters = dataFilters
    filters
  }

  override def pushedFilters(): Array[Filter] = filters

  protected def convertToExpressions(filters: Array[Filter]): Array[Expression] = {
    // todo sure schema
    val catalystExpressions = HoodieSparkUtils.convertToCatalystExpressions(filters, table.tableSchema)

    val failedExprs = catalystExpressions.zipWithIndex.filter { case (opt, _) => opt.isEmpty }
    if (failedExprs.nonEmpty) {
      val failedFilters = failedExprs.map(p => filters(p._2))
      logWarning(s"Failed to convert Filters into Catalyst expressions (${failedFilters.map(_.toString)})")
    }

    catalystExpressions.filter(_.isDefined).map(_.get).toArray
  }

  /**
   * Checks whether given expression only references partition columns
   * (and involves no sub-query)
   */
  protected def isPartitionPredicate(condition: Expression): Boolean = {
    // Validates that the provided names both resolve to the same entity
    val resolvedNameEquals = sparkSession.sessionState.analyzer.resolver

    condition.references.forall { r => table.partitionFields.exists(resolvedNameEquals(r.name, _)) } &&
      !SubqueryExpression.hasSubquery(condition)
  }
}
