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

package org.apache.hudi.common.config;

import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.secondary.index.SecondaryIndexType;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import java.util.LinkedList;
import java.util.Properties;

public class HoodieBuildTaskConfig extends HoodieConfig {

  public static final ConfigProperty<Double> LUCENE_INDEX_RAM_BUFFER_SIZE_MB = ConfigProperty
      .key("hoodie.build.task.lucene.ram.buffer.size.mb")
      .defaultValue(32.0)
      .withDocumentation("Ram buffer size from build lucene index");

  public static final ConfigProperty<Integer> LUCENE_INDEX_MERGE_FACTOR = ConfigProperty
      .key("hoodie.build.task.lucene.merge.factor")
      .defaultValue(10)
      .withDocumentation("Determines how often segment indices are merged by addDocument()");

  public static final ConfigProperty<Double> LUCENE_INDEX_MAX_MERGE_MB = ConfigProperty
      .key("hoodie.build.task.lucene.max.merge.mb")
      .defaultValue(102400.0)
      .withDocumentation("Determines the largest segment (measured by total byte size of the segment's files, in MB) that may be merged with other segments");

  public static final ConfigProperty<Boolean> LUCENE_INDEX_LOG_ENABLED = ConfigProperty
      .key("hoodie.build.task.lucene.log.enabled")
      .defaultValue(true)
      .withDocumentation("Whether to log information about merges, deletes and a message when maxFieldLength is reached");

  public static final ConfigProperty<Integer> BUILD_BATCH_ADD_SIZE = ConfigProperty
      .key("hoodie.build.batch.add.size")
      .defaultValue(1000)
      .withDocumentation("Batch size when add records to index builder");

  // ***** Bloom Index configs *****
  public static final ConfigProperty<String> BLOOM_FILTER_NUM_ENTRIES_VALUE = ConfigProperty
      .key("hoodie.index.bloom.num_entries")
      .defaultValue("60000")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "This is the number of entries to be stored in the bloom filter. "
          + "The rationale for the default: Assume the maxParquetFileSize is 128MB and averageRecordSize is 1kb and "
          + "hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. "
          + "Warning: Setting this very low, will generate a lot of false positives and index lookup "
          + "will have to scan a lot more files than it has to and setting this to a very high number will "
          + "increase the size every base file linearly (roughly 4KB for every 50000 entries). "
          + "This config is also used with DYNAMIC bloom filter which determines the initial size for the bloom.");

  public static final ConfigProperty<String> BLOOM_FILTER_FPP_VALUE = ConfigProperty
      .key("hoodie.index.bloom.fpp")
      .defaultValue("0.000000001")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "Error rate allowed given the number of entries. This is used to calculate how many bits should be "
          + "assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), "
          + "we like to tradeoff disk space for lower false positives. "
          + "If the number of entries added to bloom filter exceeds the configured value (hoodie.index.bloom.num_entries), "
          + "then this fpp may not be honored.");

  public static final ConfigProperty<String> BLOOM_INDEX_PARALLELISM = ConfigProperty
      .key("hoodie.bloom.index.parallelism")
      .defaultValue("0")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "This is the amount of parallelism for index lookup, which involves a shuffle. "
          + "By default, this is auto computed based on input workload characteristics.");

  public static final ConfigProperty<String> BLOOM_INDEX_PRUNE_BY_RANGES = ConfigProperty
      .key("hoodie.bloom.index.prune.by.ranges")
      .defaultValue("true")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "When true, range information from files to leveraged speed up index lookups. Particularly helpful, "
          + "if the key has a monotonously increasing prefix, such as timestamp. "
          + "If the record key is completely random, it is better to turn this off, since range pruning will only "
          + " add extra overhead to the index lookup.");

  public static final ConfigProperty<String> BLOOM_INDEX_USE_CACHING = ConfigProperty
      .key("hoodie.bloom.index.use.caching")
      .defaultValue("true")
      .withDocumentation("Only applies if index type is BLOOM."
          + "When true, the input RDD will cached to speed up index lookup by reducing IO "
          + "for computing parallelism or affected partitions");

  public static final ConfigProperty<Boolean> BLOOM_INDEX_USE_METADATA = ConfigProperty
      .key("hoodie.bloom.index.use.metadata")
      .defaultValue(false)
      .sinceVersion("0.11.0")
      .withDocumentation("Only applies if index type is BLOOM."
          + "When true, the index lookup uses bloom filters and column stats from metadata "
          + "table when available to speed up the process.");

  public static final ConfigProperty<String> BLOOM_INDEX_TREE_BASED_FILTER = ConfigProperty
      .key("hoodie.bloom.index.use.treebased.filter")
      .defaultValue("true")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "When true, interval tree based file pruning optimization is enabled. "
          + "This mode speeds-up file-pruning based on key ranges when compared with the brute-force mode");

  // TODO: On by default. Once stable, we will remove the other mode.
  public static final ConfigProperty<String> BLOOM_INDEX_BUCKETIZED_CHECKING = ConfigProperty
      .key("hoodie.bloom.index.bucketized.checking")
      .defaultValue("true")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "When true, bucketized bloom filtering is enabled. "
          + "This reduces skew seen in sort based bloom index lookup");

  public static final ConfigProperty<String> BLOOM_FILTER_TYPE = ConfigProperty
      .key("hoodie.bloom.index.filter.type")
      .defaultValue(BloomFilterTypeCode.DYNAMIC_V0.name())
      .withValidValues(BloomFilterTypeCode.SIMPLE.name(), BloomFilterTypeCode.DYNAMIC_V0.name())
      .withDocumentation("Filter type used. Default is BloomFilterTypeCode.DYNAMIC_V0. "
          + "Available values are [BloomFilterTypeCode.SIMPLE , BloomFilterTypeCode.DYNAMIC_V0]. "
          + "Dynamic bloom filters auto size themselves based on number of keys.");

  public static final ConfigProperty<String> BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES = ConfigProperty
      .key("hoodie.bloom.index.filter.dynamic.max.entries")
      .defaultValue("100000")
      .withDocumentation("The threshold for the maximum number of keys to record in a dynamic Bloom filter row. "
          + "Only applies if filter type is BloomFilterTypeCode.DYNAMIC_V0.");


  private String indexSaveDir;
  private SecondaryIndexType indexType;
  private LinkedList<Schema.Field> indexFields;
  private Configuration conf;

  public HoodieBuildTaskConfig(String indexSaveDir,
                               SecondaryIndexType indexType,
                               LinkedList<Schema.Field> indexFields,
                               Configuration conf) {
    this.indexSaveDir = indexSaveDir;
    this.indexType = indexType;
    this.indexFields = indexFields;
    this.conf = conf;
  }

  public HoodieBuildTaskConfig(Properties props) {
    super(props);
  }

  public double getLuceneIndexRamBufferSizeMB() {
    return getDoubleOrDefault(LUCENE_INDEX_RAM_BUFFER_SIZE_MB);
  }

  public int getLuceneIndexMergeFactor() {
    return getIntOrDefault(LUCENE_INDEX_MERGE_FACTOR);
  }

  public double getLuceneIndexMaxMergeMB() {
    return getDoubleOrDefault(LUCENE_INDEX_MAX_MERGE_MB);
  }

  public boolean isLuceneIndexLogEnabled() {
    return getBooleanOrDefault(LUCENE_INDEX_LOG_ENABLED);
  }

  public String getIndexSaveDir() {
    return indexSaveDir;
  }

  public SecondaryIndexType getIndexType() {
    return indexType;
  }

  public LinkedList<Schema.Field> getIndexFields() {
    return indexFields;
  }

  public int getBloomFilterNumEntries() {
    return getIntOrDefault(BLOOM_FILTER_NUM_ENTRIES_VALUE);
  }

  public double getBloomFilterFPP() {
    return getDoubleOrDefault(BLOOM_FILTER_FPP_VALUE);
  }

  public String getBloomFilterType() {
    return getString(BLOOM_FILTER_TYPE);
  }

  public int getDynamicBloomFilterMaxNumEntries() {
    return getInt(BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES);
  }

  public Configuration getConf() {
    return conf;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String indexSaveDir;
    private SecondaryIndexType indexType;
    private LinkedList<Schema.Field> indexFields;
    private Configuration conf;

    public Builder setIndexSaveDir(String indexSaveDir) {
      this.indexSaveDir = indexSaveDir;
      return this;
    }

    public Builder setIndexType(SecondaryIndexType indexType) {
      this.indexType = indexType;
      return this;
    }

    public Builder setIndexFields(LinkedList<Schema.Field> indexFields) {
      this.indexFields = indexFields;
      return this;
    }

    public Builder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public HoodieBuildTaskConfig build() {
      return new HoodieBuildTaskConfig(indexSaveDir, indexType, indexFields, conf);
    }
  }
}
