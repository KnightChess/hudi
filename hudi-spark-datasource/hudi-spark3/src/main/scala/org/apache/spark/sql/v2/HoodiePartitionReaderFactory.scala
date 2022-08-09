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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetPartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType


/**
 */
case class HoodiePartitionReaderFactory(partitionReaderFactory: ParquetPartitionReaderFactory, paths: Seq[String],
                                        userSpecifiedSchema: Option[StructType]) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[HoodieFileSplitPartition])
    val hoodieSplit = partition.asInstanceOf[HoodieFileSplitPartition]
    val iter = hoodieSplit.filePartition.files.iterator.map { file =>
      PartitionedFileReader(file, partitionReaderFactory.buildReader(file))
    }
    new FilePartitionReader[InternalRow](iter)
  }

}

private case class PartitionedFileReader[T](file: PartitionedFile,
                                             reader: PartitionReader[T]) extends PartitionReader[T] {
    override def next(): Boolean = reader.next()

    override def get(): T = reader.get()

    override def close(): Unit = reader.close()

    override def toString: String = file.toString
}
