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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

private[sql] object Melt {
  def of[_](ds: Dataset[_],
            ids: Seq[String],
            values: Seq[String] = Seq.empty,
            dropNulls: Boolean = false,
            variableColumnName: String = "variable",
            valueColumnName: String = "value"): DataFrame = {
    // all given ids and values should exist in ds
    val unknown = (ids ++ values).diff(ds.columns)
    if (unknown.nonEmpty) {
      throw new IllegalArgumentException(
        s"Unknown columns: ${unknown.mkString(", ")}, dataset has: ${ds.columns.mkString(", ")}"
      )
    }

    // if no values given, all non-id columns are melted
    val valueNames = if (values.isEmpty) {
      ds.columns.diff(ids).toSeq
    } else {
      values
    }

    // if there are no values given and no non-id columns exist, we cannot melt
    if (valueNames.isEmpty) {
      throw new IllegalArgumentException("The dataset has no non-id columns to melt")
    }

    // all melted values have to have the same type
    val valueTypes = ds.logicalPlan.output.filter(
      attr => valueNames.contains(attr.name)
    ).map(_.dataType).toSet
    if (valueTypes.size > 1) {
      throw new IllegalArgumentException(f"All values must be of same types, " +
        f"found: ${valueTypes.toSeq.map(_.toString).sorted.mkString(", ")}")
    }
    val valueType = valueTypes.head

    // resolve ids
    val resolver = ds.sparkSession.sessionState.analyzer.resolver
    val idAttrs = ids.map(ds.queryExecution.analyzed.resolveQuoted(_, resolver).get.toAttribute)
    val idNames = idAttrs.map(_.name)
    if (idNames.contains(variableColumnName)) {
      throw new IllegalArgumentException(f"Column name for variable column ($variableColumnName) " +
        f"must not exist among id columns: ${idNames.mkString(", ")}")
    }
    if (idNames.contains(valueColumnName)) {
      throw new IllegalArgumentException(f"Column name for value column ($valueColumnName) " +
        f"must not exist among id columns: ${idNames.mkString(", ")}")
    }

    // construct output attributes
    val variableAttr = AttributeReference(variableColumnName, StringType, nullable = false)()
    val valueAttr = AttributeReference(valueColumnName, valueType, nullable = true)()
    val output = idAttrs ++ Seq(variableAttr, valueAttr)

    // construct melt expressions for Expand
    val valueAttrs = valueNames.map(
      ds.queryExecution.analyzed.resolveQuoted(_, resolver).get.toAttribute
    )
    val exprs: Seq[Seq[Expression]] = valueAttrs.map {
      attr =>
        idAttrs ++ Seq(
          Literal(attr.name),
          attr
        )
    }

    // expand the melt expressions
    val plan = Expand(exprs, output, ds.queryExecution.logical)
    val df = Dataset.ofRows(ds.sparkSession, plan)

    // drop null values if requested
    if (dropNulls) {
      val valueColumn = col(valueColumnName)
      // with `AssertNotNull` we express this column is non-nullable (reflected in schema)
      val nonNullableValueColumn = Column(AssertNotNull(valueColumn.expr))
      df.where(valueColumn.isNotNull)
        .withColumn(valueColumnName, nonNullableValueColumn)
    } else {
      df
    }
  }
}
