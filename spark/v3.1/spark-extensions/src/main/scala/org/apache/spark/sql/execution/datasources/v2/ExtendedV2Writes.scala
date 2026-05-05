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
import org.apache.spark.sql.catalyst.plans.logical.AppendData
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression
import org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.DistributionAndOrderingUtils
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation

/**
 * Backport of the Spark 3.2 V2Writes idea to the v3.1 logical plan API. In Spark 3.1
 * AppendData/OverwriteByExpression/OverwritePartitionsDynamic do not carry a Write, so the
 * required distribution and ordering are looked up from the Iceberg table directly via
 * Spark3Util — the same helpers RewriteRowLevelOperationHelper.buildWritePlan uses.
 *
 * v3.1-only path: no Write is built here; ExtendedDataSourceV2Strategy still constructs the
 * Write at physical planning. This rule only attaches the required distribution and ordering
 * to the query feeding the write.
 *
 * Row-level commands (MERGE/UPDATE/DELETE) are intentionally skipped: their rewriters already
 * call buildWritePlan, which prepares the query before constructing AppendData/ReplaceData. The
 * Sort / RepartitionByExpression guard avoids double-wrapping that already-prepared query (and
 * also respects an explicit ORDER BY / DISTRIBUTE BY in a user-issued INSERT).
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

  // DistributionAndOrderingUtils.prepareQuery wraps its input in either a RepartitionByExpression
  // with no explicit numPartitions, or a non-global Sort over such a RepartitionByExpression. Only
  // those exact shapes are treated as already-prepared so that user code (coalesce, repartition(N),
  // global ORDER BY, sortWithinPartitions on a non-shuffled source) is left untouched.
  private def alreadyPrepared(query: LogicalPlan): Boolean = query match {
    case Sort(_, false, RepartitionByExpression(_, _, None)) => true
    case RepartitionByExpression(_, _, None) => true
    case _ => false
  }

  private def prepareQuery(r: DataSourceV2Relation, query: LogicalPlan): LogicalPlan = {
    val icebergTable = Spark3Util.toIcebergTable(r.table)
    val distribution = Spark3Util.buildRequiredDistribution(icebergTable)
    val ordering = Spark3Util.buildRequiredOrdering(distribution, icebergTable)
    DistributionAndOrderingUtils.prepareQuery(distribution, ordering, query, conf)
  }
}
