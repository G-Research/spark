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

import org.apache.spark.sql.catalyst.analysis.{AnsiTypeCoercion, TypeCoercion, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StringType}

private[sql] object Melt {
  def of[_](
      ds: Dataset[_],
      ids: Seq[String],
      values: Seq[String],
      dropNulls: Boolean,
      variableColumnName: String,
      valueColumnName: String): DataFrame = {
    // values should be disjoint to ids
    if (values.intersect(ids).nonEmpty) {
      throw new AnalysisException("MELT_ID_AND_VALUE_COLUMNS_NOT_DISJOINT", Array(
        ids.mkString(", "), values.mkString(", "), values.intersect(ids).mkString(", ")
      ))
    }

    // id columns must not contain variable or value column name
    if (ids.contains(variableColumnName)) {
      throw new AnalysisException("MELT_VARIABLE_COLUMN_IS_ID_COLUMN", Array(
        variableColumnName, ids.mkString(", ")
      ))
    }
    if (ids.contains(valueColumnName)) {
      throw new AnalysisException("MELT_VALUE_COLUMN_IS_ID_COLUMN", Array(
        valueColumnName, ids.mkString(", ")
      ))
    }

    // if no values given, all non-id columns are melted
    val valueNameCandidates = ds.columns.diff(ids).toSeq
    val valueNames = Some(values).filter(_.nonEmpty).getOrElse(valueNameCandidates)

    // if there are no values given and no non-id columns exist, we cannot melt
    if (valueNames.isEmpty) {
      throw new AnalysisException("MELT_REQUIRES_VALUE_COLUMNS", Array(ids.mkString(", ")))
    }

    // resolve given id and value column names
    val idExprs = ids.map(resolve(ds, _))
    val valueExprs = valueNames.map(resolve(ds, _))

    // all given ids and values should exist in ds
    val unresolvedIds = idExprs.filter(_.isLeft).map(_.swap.toOption.get)
    if (unresolvedIds.nonEmpty) {
      val idNameCandidates = ds.columns.diff(valueNames).toSeq
      throw new AnalysisException("MISSING_COLUMNS",
        Array(unresolvedIds.mkString(", "), idNameCandidates.mkString(", ")))
    }
    val unresolvedValues = valueExprs.filter(_.isLeft).map(_.swap.toOption.get)
    if (unresolvedValues.nonEmpty) {
      throw new AnalysisException("MISSING_COLUMNS",
        Array(unresolvedValues.mkString(", "), valueNameCandidates.mkString(", ")))
    }

    // all melted values have to have the same type
    val resolvedValues = valueExprs.filter(_.isRight).map(_.toOption.get)
    val valueType = resolvedValues.map(_.dataType).reduce(tightestCommonType)

    // construct output attributes
    val idAttrs = idExprs.filter(_.isRight).map(_.toOption.get.toAttribute)
    val variableAttr = AttributeReference(variableColumnName, StringType, nullable = false)()
    // even with dropNulls == true we output nullable value column
    // and filter for valueColumn.isNotNull after expand, see `if (dropNulls)` below
    val valueAttr = AttributeReference(valueColumnName, valueType, nullable = true)()
    val output = idAttrs ++ Seq(variableAttr, valueAttr)

    // construct melt expressions for Expand
    val exprs: Seq[Seq[Expression]] = valueNames.map {
      value =>
        idAttrs ++ Seq(
          Literal(value),
          Cast(UnresolvedAttribute.quotedString(value), valueType)
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

  /**
   * Resolves the column against the given dataset, returns either the resolved NamedExpression,
   * or the column name that could not be resolved.
   *
   * @param ds Dataset
   * @param column column name
   * @tparam _ Dataset type
   * @return either resolved NamedExpression or column name
   */
  private def resolve[_](ds: Dataset[_], column: String): Either[String, NamedExpression] = {
    val resolver = ds.sparkSession.sessionState.analyzer.resolver
    ds.logicalPlan.resolveQuoted(column, resolver).map(_.toAttribute) match {
      case Some(v) => Right(v)
      case None => Left(column)
    }
  }

  private def tightestCommonType(d1: DataType, d2: DataType): DataType = {
    val typeCoercion = if (SQLConf.get.ansiEnabled) AnsiTypeCoercion else TypeCoercion
    typeCoercion.findTightestCommonType(d1, d2).getOrElse(
      throw new AnalysisException("MELT_VALUE_DATA_TYPE_MISMATCH",
        Array(d1.simpleString, d2.simpleString))
    )
  }

}
