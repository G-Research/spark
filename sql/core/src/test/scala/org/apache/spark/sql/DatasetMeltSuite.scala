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

import org.apache.spark.sql.errors.QueryErrorsSuiteBase
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Comprehensive tests for Dataset.melt.
 */
class DatasetMeltSuite extends QueryTest
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

  val meltedSchema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("var", StringType, nullable = false),
    StructField("val", StringType, nullable = true)
  ))

  test("overloaded melt without values") {
    val ds = meltWideDataDs.select($"id", $"str1", $"str2")
    checkAnswer(
      ds.melt(Array($"id"), "var", "val"),
      ds.melt(Array($"id"), Array.empty, "var", "val")
    )
  }

  test("melt with single id") {
    val melted = meltWideDataDs
      .melt(
        Array($"id"),
        Array($"str1", $"str2"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    melted.explain(true)
    assert(melted.schema === meltedSchema)
    checkAnswer(melted, meltedWideDataRows)
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

    val melted = meltWideDataDs
      .melt(
        Array($"id", $"int1"),
        Array($"str1", $"str2"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    assert(melted.schema === StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("int1", IntegerType, nullable = true),
      StructField("var", StringType, nullable = false),
      StructField("val", StringType, nullable = true)
    )))
    checkAnswer(melted, meltedRows)
  }

  test("melt without ids") {
    val melted = meltWideDataDs
      .melt(
        Array.empty,
        Array($"str1", $"str2"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    assert(melted.schema === StructType(Seq(
      StructField("var", StringType, nullable = false),
      StructField("val", StringType, nullable = true)
    )))
    checkAnswer(melted, meltedWideDataWithoutIdRows)
  }

  test("melt without values") {
    val melted = meltWideDataDs.select($"id", $"str1", $"str2")
      .melt(
        Array($"id"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    assert(melted.schema === meltedSchema)
    checkAnswer(melted, meltedWideDataRows)
  }

  test("melt without ids or values") {
    // do not drop nulls
    val melted = meltWideDataDs.select($"str1", $"str2")
      .melt(
        Array.empty,
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val"
      )
    assert(melted.schema === StructType(Seq(
      StructField("var", StringType, nullable = false),
      StructField("val", StringType, nullable = true)
    )))
    checkAnswer(melted, meltedWideDataWithoutIdRows)
  }

  test("melt with variable / value value columns") {
    // with value column `variable` and `value`
    val melted = meltWideDataDs
      .withColumnRenamed("str1", "var")
      .withColumnRenamed("str2", "val")
      .melt(
        Array($"id"),
        Array($"var", $"val"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    checkAnswer(melted, meltedWideDataRows.map(row => Row(
      row.getInt(0),
      row.getString(1) match {
        case "str1" => "var"
        case "str2" => "val"
      },
      row.getString(2)
    )))

    // with un-referenced column `variable` and `value`
    val melted2 = meltWideDataDs
      .withColumnRenamed("int1", "var")
      .withColumnRenamed("long1", "val")
      .melt(
        Array($"id"),
        Array($"str1", $"str2"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    checkAnswer(melted2, meltedWideDataRows)
  }

  test("melt with incompatible value types") {
    val e = intercept[AnalysisException] {
      meltWideDataDs.melt(
        Array($"id"),
        Array($"str1", $"int1"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e,
      errorClass = "MELT_VALUE_DATA_TYPE_MISMATCH",
      msg = "Melt value columns must have compatible data types, " +
        "but string and int are not compatible"
    )
  }

  test("melt with compatible value types") {
    val melted = meltWideDataDs.melt(
      Array($"id"),
      Array($"int1", $"long1"),
      variableColumnName = "var",
      valueColumnName = "val"
    )
    assert(melted.schema === StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("var", StringType, nullable = false),
      StructField("val", LongType, nullable = true)
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
    checkAnswer(melted, meltedRows)
  }

  test("melt with invalid arguments") {
    // melting where id column does not exist
    val e1 = intercept[AnalysisException] {
      meltWideDataDs.melt(
        Array($"1", $"2"),
        Array($"str1", $"str2"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e1,
      errorClass = "MISSING_COLUMN",
      msg = "Column '`1`' does not exist\\. Did you mean one " +
        "of the following\\? \\[id, int1, str1, str2, long1\\];(\n.*)*",
      matchMsg = true
    )

    // melting where value column does not exist
    val e2 = intercept[AnalysisException] {
      meltWideDataDs.melt(
        Array($"id"),
        Array($"does", $"not", $"exist"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e2,
      errorClass = "MISSING_COLUMN",
      msg = "Column 'does' does not exist\\. Did you mean one " +
        "of the following\\? \\[id, int1, long1, str1, str2\\];(\n.*)*",
      matchMsg = true
    )

    // melting with column in both ids and values
    val e3 = intercept[AnalysisException] {
      meltWideDataDs.melt(
        Array($"id", $"str1", $"int1"),
        Array($"str1", $"str2", $"int1", $"long1"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e3,
      errorClass = "MELT_ID_AND_VALUE_COLUMNS_NOT_DISJOINT",
      msg = "The melt id columns \\[id#\\d+, str1#\\d+, int1#\\d+\\] " +
        "and value columns \\[str1#\\d+, str2#\\d+, int1#\\d+, long1#\\d+L\\] " +
        "must be disjoint, but these columns are both: \\[str1#\\d+, int1#\\d+\\]",
      matchMsg = true
    )

    // melting with empty list of value columns
    // where potential value columns are of incompatible types
    val e4 = intercept[AnalysisException] {
      meltWideDataDs.melt(
        Array.empty,
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e4,
      errorClass = "MELT_VALUE_DATA_TYPE_MISMATCH",
      msg = "Melt value columns must have compatible data types, " +
        "but int and string are not compatible"
    )

    val e5 = intercept[AnalysisException] {
      meltWideDataDs.melt(
        Array($"id"),
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e5,
      errorClass = "MELT_VALUE_DATA_TYPE_MISMATCH",
      msg = "Melt value columns must have compatible data types, " +
        "but string and int are not compatible"
    )

    // melting without giving values and no non-id columns
    val e6 = intercept[AnalysisException] {
      meltWideDataDs.select($"id", $"str1", $"str2").melt(
        Array($"id", $"str1", $"str2"),
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e6,
      errorClass = "MELT_REQUIRES_VALUE_COLUMNS",
      msg = "At least one non-id column is required to melt. " +
        "All columns are id columns: \\[id#\\d+, str1#\\d+, str2#\\d+\\]",
      matchMsg = true
    )
  }

  test("melt after pivot") {
    // see test "pivot courses" in DataFramePivotSuite
    val pivoted = courseSales.groupBy("year").pivot("course", Array("dotNET", "Java"))
      .agg(sum($"earnings"))
    val melted = pivoted.melt(Array($"year"), "course", "earnings")
    val expected = courseSales.groupBy("year", "course").sum("earnings")
    checkAnswer(melted, expected)
  }

  test("melt with dot and backtick") {
    val ds = meltWideDataDs
      .withColumnRenamed("id", "an.id")
      .withColumnRenamed("str1", "str.one")
      .withColumnRenamed("str2", "str.two")

    val melted = ds.melt(
        Array($"`an.id`"),
        Array($"`str.one`", $"`str.two`"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    checkAnswer(melted, meltedWideDataRows.map(row => Row(
        row.getInt(0),
        row.getString(1) match {
          case "str1" => "str.one"
          case "str2" => "str.two"
        },
        row.getString(2)
      ))
    )

    // without backticks, this references struct fields, which do not exist
    val e = intercept[AnalysisException] {
      ds.melt(
        Array($"an.id"),
        Array($"str.one", $"str.two"),
        variableColumnName = "var",
        valueColumnName = "val"
      ).collect()  // TODO: check if collect is really needed
    }
    checkErrorClass(
      exception = e,
      errorClass = "MISSING_COLUMN",
      msg = "Column 'an.id' does not exist\\. Did you mean one " +
        "of the following\\? \\[an.id, int1, long1, str.one, str.two\\];(\n.*)*",
      matchMsg = true
    )
  }

  /** TODO(SPARK-39292): Would be nice to melt on struct fields.
  test("SPARK-39292: melt with struct fields") {
    val ds = meltWideDataDs.select(
      struct($"id").as("an"),
      struct(
        $"str1".as("one"),
        $"str2".as("two")
      ).as("str")
    )
    ds.select($"an.id", $"str.one", $"str.two").show()

    checkAnswer(
      ds.melt(Array($"an.id"), Array($"str.one", $"str.two"), "var", "val"),
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

case class WideData(id: Int, str1: String, str2: String, int1: Option[Int], long1: Option[Long])
