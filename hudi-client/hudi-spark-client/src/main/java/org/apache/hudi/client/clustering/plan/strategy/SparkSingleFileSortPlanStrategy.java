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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.PartitionAwareClusteringPlanStrategy;

import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * In this strategy, clustering group for each partition is built in the same way as {@link SparkSizeBasedClusteringPlanStrategy}.
 * The difference is that the output groups is 1 and file group id remains the same.
 */
public class SparkSingleFileSortPlanStrategy<T extends HoodieRecordPayload<T>>
    extends PartitionAwareClusteringPlanStrategy<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  public SparkSingleFileSortPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected Map<String, String> getStrategyParams() {
    Map<String, String> params = new HashMap<>();
    if (!StringUtils.isNullOrEmpty(getWriteConfig().getClusteringSortColumns())) {
      params.put(PLAN_STRATEGY_SORT_COLUMNS.key(), getWriteConfig().getClusteringSortColumns());
    }
    return params;
  }

  @Override
  protected Stream<HoodieClusteringGroup> buildClusteringGroupsForPartition(String partitionPath, List<FileSlice> fileSlices) {
    List<Pair<List<FileSlice>, Integer>> fileSliceGroups = fileSlices.stream()
        .map(fileSlice -> Pair.of(Collections.singletonList(fileSlice), 1)).collect(Collectors.toList());
    return fileSliceGroups.stream().map(fileSliceGroup -> HoodieClusteringGroup.newBuilder()
        .setSlices(getFileSliceInfo(fileSliceGroup.getLeft()))
        .setNumOutputFileGroups(fileSliceGroup.getRight())
        .setMetrics(buildMetrics(fileSliceGroup.getLeft()))
        .build());
  }
}
