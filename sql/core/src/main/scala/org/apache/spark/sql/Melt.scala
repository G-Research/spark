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
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object Melt {
  def of[_](ds: Dataset[_],
            ids: Seq[String],
            values: Seq[String],
            dropNulls: Boolean = true,
            variableColumnName: String = "variable",
            valueColumnName: String = "value"): DataFrame = {
    if (ids.isEmpty) {
      throw new IllegalArgumentException(f"At least one id column required")
    }
    if (values.isEmpty) {
      throw new IllegalArgumentException(f"At least one value column required")
    }

    val valueTypes = ds.schema.fields
      .filter(attr => values.contains(attr.name))
      .map(_.dataType).toSet
    if (valueTypes.size > 1) {
      throw new IllegalArgumentException(f"All values must be of same types, " +
        f"found: ${valueTypes.mkString(", ")}")
    }
    val valueType = valueTypes.head

    val idAttrs = ds.logicalPlan.output.filter(attr => ids.contains(attr.name))
    val idNames = idAttrs.map(_.name)
    if (idNames.contains(variableColumnName)) {
      throw new IllegalArgumentException(f"Column name for variable column ($variableColumnName) " +
        f"must not exist among id columns: ${idNames.mkString(", ")}")
    }
    if (idNames.contains(valueColumnName)) {
      throw new IllegalArgumentException(f"Column name for value column ($valueColumnName) " +
        f"must not exist among id columns: ${idNames.mkString(", ")}")
    }

    val variableAttr = AttributeReference(variableColumnName, StringType, nullable = false)()
    val valueAttr = AttributeReference(valueColumnName, valueType, nullable = true)()
    val output = idAttrs ++ Seq(variableAttr, valueAttr)

    val valueAttrs = ds.queryExecution.logical.output.filter(attr => values.contains(attr.name))
    val exprs: Seq[Seq[Expression]] = valueAttrs.map {
      attr =>
        idAttrs ++ Seq(
          Literal(attr.name),
          attr
        )
    }

    val plan = Expand(exprs, output, ds.queryExecution.logical)
    val df = Dataset.ofRows(ds.sparkSession, plan)

    if (dropNulls) {
      df.where(col(valueColumnName).isNotNull)
    } else {
      df
    }
  }
}
