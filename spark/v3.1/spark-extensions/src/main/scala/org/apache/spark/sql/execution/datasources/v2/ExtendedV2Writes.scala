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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.util.SortOrderUtil
import org.apache.spark.sql.catalyst.plans.logical.AppendData
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression
import org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.DistributionAndOrderingUtils
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation
import org.apache.spark.sql.connector.iceberg.distributions.Distribution
import org.apache.spark.sql.connector.iceberg.distributions.Distributions
import org.apache.spark.sql.connector.iceberg.distributions.OrderedDistribution
import org.apache.spark.sql.connector.iceberg.expressions.SortOrder

/**
 * Backport of Spark 3.2's V2Writes idea for v3.1's AppendData/OverwriteByExpression/
 * OverwritePartitionsDynamic. Attaches a local Sort to the query feeding the write when the
 * table has an explicit sort order or RANGE distribution; never attaches an Exchange.
 *
 * No distribution: Spark 3.1 only has strict RepartitionByExpression. Spark 3.4+'s
 * RebalancePartitions (the non-strict node Spark 3.5's V2Writes emits for Iceberg) doesn't
 * exist here, and forcing a strict repartition would turn skewed partition keys into stragglers.
 *
 * No synthesized partition-spec sort: matches Spark 3.5. When a sort is attached, partition
 * cols are prepended via SortOrderUtil so ClusteredDataWriter sees per-task clustering;
 * unsorted partitioned tables need fanout (or pre-clustering by the user).
 *
 * MERGE/UPDATE/DELETE are skipped — RewriteRowLevelOperationHelper.buildWritePlan already
 * prepares those queries; alreadyPrepared() detects its output shape to avoid double-wrapping.
 */
object ExtendedV2Writes extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case a @ AppendData(r: DataSourceV2Relation, query, _, _)
      if isIcebergRelation(r) && !alreadyPrepared(query) =>
      a.withNewQuery(prepareQuery(r, query))

    case o @ OverwriteByExpression(r: DataSourceV2Relation, _, query, _, _)
      if isIcebergRelation(r) && !alreadyPrepared(query) =>
      o.withNewQuery(prepareQuery(r, query))

    case o @ OverwritePartitionsDynamic(r: DataSourceV2Relation, query, _, _)
      if isIcebergRelation(r) && !alreadyPrepared(query) =>
      o.withNewQuery(prepareQuery(r, query))
  }

  // Matches the shapes RewriteRowLevelOperationHelper.buildWritePlan produces. Bare
  // Sort(_, false, _) is intentionally NOT matched — it would swallow a user's
  // sortWithinPartitions on the wrong columns and skip the table-required ordering.
  private def alreadyPrepared(query: LogicalPlan): Boolean = query match {
    case Sort(_, false, RepartitionByExpression(_, _, None)) => true
    case RepartitionByExpression(_, _, None) => true
    case _ => false
  }

  private def prepareQuery(r: DataSourceV2Relation, query: LogicalPlan): LogicalPlan = {
    val icebergTable = Spark3Util.toIcebergTable(r.table)
    // Distribution is computed only so requiredOrdering can read OrderedDistribution.ordering;
    // we then pass unspecified() so no Exchange is attached. See class doc.
    val tableDistribution = Spark3Util.buildRequiredDistribution(icebergTable)
    val ordering = requiredOrdering(tableDistribution, icebergTable)
    DistributionAndOrderingUtils.prepareQuery(
      Distributions.unspecified(), ordering, query, conf)
  }

  // Sort attached only for RANGE (OrderedDistribution) or an explicit table sort order. Use
  // SortOrderUtil.buildSortOrder so partition cols are prepended — sorting by user fields
  // alone won't cluster a partition transform (e.g. `id` doesn't cluster `bucket(N, id)`).
  private def requiredOrdering(
      distribution: Distribution,
      icebergTable: org.apache.iceberg.Table): Array[SortOrder] = {
    distribution match {
      case od: OrderedDistribution =>
        od.ordering
      case _ if !icebergTable.sortOrder().isUnsorted =>
        Spark3Util.convert(SortOrderUtil.buildSortOrder(icebergTable))
      case _ =>
        Array.empty[SortOrder]
    }
  }
}
