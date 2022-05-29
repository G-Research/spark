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

import scala.reflect.ClassTag

import org.apache.spark.sql.errors.QueryErrorsSuiteBase
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

/**
 * Comprehensive tests for Melt.of(), which is used by Dataset.melt.
 */
class MeltSuite extends QueryTest
  with QueryErrorsSuiteBase
  with SharedSparkSession {
  import testImplicits._

  lazy val meltWideDataDs: Dataset[WideData] = Seq(
    WideData(1, "one", "One", Some(1), Some(1L)),
    WideData(2, "two", null, None, Some(2L)),
    WideData(3, null, "three", Some(3), None),
    WideData(4, null, null, None, None)
  ).toDS()

  val meltedWideDataRows = Seq(
    Row(1, "str1", "one"),
    Row(1, "str2", "One"),
    Row(2, "str1", "two"),
    Row(2, "str2", null),
    Row(3, "str1", null),
    Row(3, "str2", "three"),
    Row(4, "str1", null),
    Row(4, "str2", null)
  )

  val meltedWideDataWithoutIdRows: Seq[Row] =
    meltedWideDataRows.map(row => Row(row.getString(1), row.getString(2)))

  private def assertException[T <: Exception : ClassTag](func: => Any)(message: String): Unit = {
    val exception = intercept[T] { func }
    assert(exception.getMessage === message)
  }

  test("melt without ids or values") {
    // do not drop nulls
    checkAnswer(
      Melt.of(
        meltWideDataDs.select($"str1", $"str2"),
        Seq.empty,
        Seq.empty,
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataWithoutIdRows
    )

    // drop nulls
    checkAnswer(
      Melt.of(
        meltWideDataDs.select($"str1", $"str2"),
        Seq.empty,
        Seq.empty,
        dropNulls = true,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataWithoutIdRows.filter(row => !row.isNullAt(1))
    )
  }

  test("melt without ids") {
    // do not drop nulls
    checkAnswer(
      Melt.of(
        meltWideDataDs.select($"str1", $"str2"),
        Seq.empty,
        Seq("str1", "str2"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataWithoutIdRows
    )

    // drop nulls
    checkAnswer(
      Melt.of(
        meltWideDataDs.select($"str1", $"str2"),
        Seq.empty,
        Seq("str1", "str2"),
        dropNulls = true,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataWithoutIdRows.filter(row => !row.isNullAt(1))
    )
  }

  test("melt with single id") {
    // do not drop nulls
    checkAnswer(
      Melt.of(
        meltWideDataDs,
        Seq("id"),
        Seq("str1", "str2"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataRows
    )

    // drop nulls
    checkAnswer(
      Melt.of(
        meltWideDataDs,
        Seq("id"),
        Seq("str1", "str2"),
        dropNulls = true,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataRows.filter(row => !row.isNullAt(2))
    )
  }

  test("melt with two ids") {
    val meltedRows = Seq(
      Row(1, 1, "str1", "one"),
      Row(1, 1, "str2", "One"),
      Row(2, null, "str1", "two"),
      Row(2, null, "str2", null),
      Row(3, 3, "str1", null),
      Row(3, 3, "str2", "three"),
      Row(4, null, "str1", null),
      Row(4, null, "str2", null)
    )

    // do not drop nulls
    checkAnswer(
      Melt.of(
        meltWideDataDs,
        Seq("id", "int1"),
        Seq("str1", "str2"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedRows
    )

    // drop nulls
    checkAnswer(
      Melt.of(
        meltWideDataDs,
        Seq("id", "int1"),
        Seq("str1", "str2"),
        dropNulls = true,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedRows.filter(row => !row.isNullAt(3))
    )
  }

  test("melt without values") {
    // do not drop nulls
    checkAnswer(
      Melt.of(
        meltWideDataDs.select($"id", $"str1", $"str2"),
        Seq("id"),
        Seq.empty,
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataRows
    )

    // do drop nulls
    checkAnswer(
      Melt.of(
        meltWideDataDs.select($"id", $"str1", $"str2"),
        Seq("id"),
        Seq.empty,
        dropNulls = true,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataRows.filter(row => !row.isNullAt(2))
    )
  }

  test("melt with variable / value value columns") {
    // with value column `variable` and `value`
    checkAnswer(
      Melt.of(
        meltWideDataDs
          .withColumnRenamed("str1", "variable")
          .withColumnRenamed("str2", "value"),
        Seq("id"),
        Seq("variable", "value"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataRows.map(row => Row(
        row.getInt(0),
        row.getString(1) match {
          case "str1" => "variable"
          case "str2" => "value"
        },
        row.getString(2)
      ))
    )

    // with un-referenced column `variable` and `value`
    checkAnswer(
      Melt.of(
        meltWideDataDs
          .withColumnRenamed("int1", "variable")
          .withColumnRenamed("long1", "value"),
        Seq("id"),
        Seq("str1", "str2"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataRows
    )
  }

  test("melt with incompatible value types") {
    val e = intercept[AnalysisException] {
      Melt.of(
        meltWideDataDs,
        Seq("id"),
        Seq("str1", "int1"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value")
    }
    checkErrorClass(
      exception = e,
      errorClass = "MELT_VALUE_DATA_TYPE_MISMATCH",
      msg = "Melt value columns must have compatible data types, " +
        "but string and int are not compatible"
    )
  }

  test("melt with compatible value types") {
    // do not drop nulls
    val df = Melt.of(
      meltWideDataDs,
      Seq("id"),
      Seq("int1", "long1"),
      dropNulls = false,
      variableColumnName = "variable",
      valueColumnName = "value")

    assert(df.schema === StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("variable", StringType, nullable = false),
      StructField("value", LongType, nullable = true)
    )))

    val meltedRows = Seq(
      Row(1, "int1", 1L),
      Row(1, "long1", 1L),
      Row(2, "int1", null),
      Row(2, "long1", 2L),
      Row(3, "int1", 3L),
      Row(3, "long1", null),
      Row(4, "int1", null),
      Row(4, "long1", null)
    )

    checkAnswer(df, meltedRows)

    // drop nulls
    val df2 = Melt.of(
      meltWideDataDs,
      Seq("id"),
      Seq("int1", "long1"),
      dropNulls = true,
      variableColumnName = "variable",
      valueColumnName = "value")

    assert(df2.schema === StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("variable", StringType, nullable = false),
      StructField("value", LongType, nullable = false)
    )))

    checkAnswer(df2, meltedRows.filter(row => !row.isNullAt(2)))
  }

  test("melt with invalid arguments") {
    // melting where id column does not exist
    val e1 = intercept[AnalysisException] {
      Melt.of(
        meltWideDataDs,
        Seq("1", "2"),
        Seq("str1", "str2"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value")
    }
    checkErrorClass(
      exception = e1,
      errorClass = "MISSING_COLUMNS",
      msg = "Columns [1, 2] do not exist. Did you mean any of the following? [id, int1, long1]"
    )

    // melting where value column does not exist
    val e2 = intercept[AnalysisException] {
      Melt.of(
        meltWideDataDs,
        Seq("id"),
        Seq("does", "not", "exist"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value")
    }
    checkErrorClass(
      exception = e2,
      errorClass = "MISSING_COLUMNS",
      msg = "Columns [does, not, exist] do not exist. Did you mean any of the following? " +
        "[str1, str2, int1, long1]"
    )

    // melting with column in both ids and values
    val e3 = intercept[AnalysisException] {
      Melt.of(
        meltWideDataDs,
        Seq("id", "str1", "int1"),
        Seq("str1", "str2", "int1", "long1"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value")
    }
    checkErrorClass(
      exception = e3,
      errorClass = "MELT_ID_AND_VALUE_COLUMNS_NOT_DISJOINT",
      msg = "The melt id columns [id, str1, int1] and value columns [str1, str2, int1, long1] " +
        "must be disjoint, but these columns are both: [str1, int1]"
    )

    // melting with empty list of value columns
    // where potential value columns are of incompatible types
    val e4 = intercept[AnalysisException] {
      Melt.of(
        meltWideDataDs,
        Seq.empty,
        Seq.empty,
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value")
    }
    checkErrorClass(
      exception = e4,
      errorClass = "MELT_VALUE_DATA_TYPE_MISMATCH",
      msg = "Melt value columns must have compatible data types, " +
        "but int and string are not compatible"
    )

    val e5 = intercept[AnalysisException] {
      Melt.of(
        meltWideDataDs,
        Seq("id"),
        Seq.empty,
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value")
    }
    checkErrorClass(
      exception = e5,
      errorClass = "MELT_VALUE_DATA_TYPE_MISMATCH",
      msg = "Melt value columns must have compatible data types, " +
        "but string and int are not compatible"
    )

    // melting without giving values and no non-id columns
    val e6 = intercept[AnalysisException] {
      Melt.of(
        meltWideDataDs.select("id", "str1", "str2"),
        Seq("id", "str1", "str2"),
        Seq.empty,
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value")
    }
    checkErrorClass(
      exception = e6,
      errorClass = "MELT_REQUIRES_VALUE_COLUMNS",
      msg = "At least one non-id column is required to melt. " +
        "All columns are id columns: [id, str1, str2]"
    )

    // melting with id column `variable`
    val e7 = intercept[AnalysisException] {
      Melt.of(
        meltWideDataDs.withColumn("var", $"id"),
        Seq("id", "var"),
        Seq("str1", "str2"),
        dropNulls = false,
        variableColumnName = "var",
        valueColumnName = "val")
    }
    checkErrorClass(
      exception = e7,
      errorClass = "MELT_VARIABLE_COLUMN_IS_ID_COLUMN",
      msg = "The melt variable column name 'var' must not be part of the id column names: [id, var]"
    )
    checkAnswer(
      Melt.of(
        meltWideDataDs.withColumn("var", $"id"),
        Seq("id", "var"),
        Seq("str1", "str2"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataRows.map( row => Row(row(0), row(0), row(1), row(2)))
    )

    // melting with id column `value`
    val e8 = intercept[AnalysisException] {
      Melt.of(
        meltWideDataDs.withColumn("val", $"id"),
        Seq("id", "val"),
        Seq("str1", "str2"),
        dropNulls = false,
        variableColumnName = "var",
        valueColumnName = "val")
    }
    checkErrorClass(
      exception = e8,
      errorClass = "MELT_VALUE_COLUMN_IS_ID_COLUMN",
      msg = "The melt value column name 'val' must not be part of the id column names: [id, val]"
    )
    checkAnswer(
      Melt.of(
        meltWideDataDs.withColumn("val", $"id"),
        Seq("id", "val"),
        Seq("str1", "str2"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataRows.map( row => Row(row(0), row(0), row(1), row(2)))
    )
  }

  test("melt with dot and backtick") {
    val df = meltWideDataDs
      .withColumnRenamed("id", "an.id")
      .withColumnRenamed("str1", "str.one")
      .withColumnRenamed("str2", "str.two")
    checkAnswer(
      Melt.of(
        df,
        Seq("`an.id`"),
        Seq("`str.one`", "`str.two`"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value"),
      meltedWideDataRows.map(row => Row(
        row.getInt(0),
        row.getString(1) match {
          case "str1" => "`str.one`"
          case "str2" => "`str.two`"
        },
        row.getString(2)
      ))
    )

    // without backticks, this references struct fields, which do not exist
    val e = intercept[AnalysisException] {
      Melt.of(df,
        Seq("an.id"),
        Seq("str.one", "str.two"),
        dropNulls = false,
        variableColumnName = "variable",
        valueColumnName = "value").collect()
    }
    checkErrorClass(
      exception = e,
      errorClass = "MISSING_COLUMNS",
      msg = "Columns [an.id] do not exist. Did you mean any of the following? [an.id, int1, long1]"
    )
  }

  /** TODO(SPARK-39292): Would be nice to melt on struct fields.
  test("SPARK-39292: melt with struct fields") {
    val df = meltWideDataDs.select(
      struct($"id").as("an"),
      struct(
        $"str1".as("one"),
        $"str2".as("two")
      ).as("str")
    )

    checkAnswer(
      Melt.of(df, Seq("an.id"), Seq("str.one", "str.two"), false, "variable", "value"),
      meltedWideDataRows.map(row => Row(
        row.getInt(0),
        row.getString(1) match {
          case "str1" => "str.one"
          case "str2" => "str.two"
        },
        row.getString(2)
      ))
    )
  } */
}
