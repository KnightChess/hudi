/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.secondary.index.bloom;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.config.HoodieBuildTaskConfig;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.secondary.index.ISecondaryIndexBuilder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public class BloomIndexBuilder implements ISecondaryIndexBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(BloomIndexBuilder.class);

  private final String name;

  private final LinkedList<Schema.Field> indexFields;

  private final Configuration conf;

  private final Type.TypeID[] fieldTypes;

  private final Path indexSaveFilePath;

  private BloomFilter bloomFilter;

  private FSDataOutputStream out;

  private final StringBuilder reuseKeyBuilder;

  //TODO merge small by partition
  public BloomIndexBuilder(HoodieBuildTaskConfig indexConfig) {
    this.bloomFilter = BloomFilterFactory.createBloomFilter(indexConfig.getBloomFilterNumEntries(), indexConfig.getBloomFilterFPP(),
        indexConfig.getDynamicBloomFilterMaxNumEntries(), indexConfig.getBloomFilterType());
    this.name = "bloom-index-builder-" + System.nanoTime();
    this.indexFields = indexConfig.getIndexFields();
    this.conf = indexConfig.getConf();
    // FileID directory or file
    this.indexSaveFilePath = new Path(indexConfig.getIndexSaveDir());
    // simple write json, lazy init
    this.out = null;
    this.reuseKeyBuilder = new StringBuilder();

    List<String> fieldNames = new ArrayList<>();
    fieldTypes = new Type.TypeID[indexFields.size()];
    IntStream.range(0, indexFields.size()).forEach(i -> {
      Schema.Field field = indexFields.get(i);
      fieldTypes[i] = AvroInternalSchemaConverter.buildTypeFromAvroSchema(field.schema()).typeId();
      fieldNames.add(field.name());
    });
    LOG.info("Init bloom index builder ok, name: {}, indexFields: {}", name, fieldNames);

  }

  @Override
  public void addBatch(GenericRecord[] records, int size) throws IOException {
    for (int i = 0; i < size; i++) {
      addRow(records[i]);
    }
  }

  @Override
  public void addRow(GenericRecord record) throws IOException {
    buildKey(reuseKeyBuilder, record);
    bloomFilter.add(reuseKeyBuilder.toString());
  }

  @Override
  public String getName() {
    return name;
  }

  private void buildKey(StringBuilder reuseKeyBuilder, GenericRecord record) {
    reuseKeyBuilder.setLength(0);
    IntStream.range(0, indexFields.size()).forEach(i -> {
      String fieldName = indexFields.get(i).name();
      Object fieldValue = record.get(fieldName);
      if (Objects.isNull(fieldValue)) {
        reuseKeyBuilder.append("__null__");
      } else {
        reuseKeyBuilder.append(fieldValue);
      }
    });
  }

  @Override
  public void close() {
    try {
      if (out == null) {
        FileSystem fs = indexSaveFilePath.getFileSystem(new Configuration());
        this.out = fs.create(indexSaveFilePath, true);
      }
      out.writeChars(bloomFilter.serializeToString());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        out.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
