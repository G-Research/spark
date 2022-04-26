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

import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.test.SharedSparkSession

import scala.reflect.ClassTag

/**
 * Comprehensive tests for Melt.of(), which is used by Dataset.melt.
 */
class MeltSuite extends QueryTest
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

  private def assertException[T <: Exception : ClassTag](func: => Any)(message: String): Unit = {
    val exception = intercept[T] { func }
    assert(exception.getMessage === message)
  }

  test("melt without ids or values") {
    // do not drop nulls
    checkAnswer(
      Melt.of(meltWideDataDs.select($"str1", $"str2"), Seq.empty),
      meltedWideDataRows.map(row => Row(row.getString(1), row.getString(2)))
    )

    // drop nulls
    checkAnswer(
      Melt.of(meltWideDataDs.select($"str1", $"str2"), Seq.empty, dropNulls = true),
      meltedWideDataRows.filter(row => !row.isNullAt(2))
        .map(row => Row(row.getString(1), row.getString(2)))
    )
  }

  test("melt without ids") {
    // do not drop nulls
    checkAnswer(
      Melt.of(meltWideDataDs.select($"str1", $"str2"), Seq.empty, Seq("str1", "str2")),
      meltedWideDataRows.map(row => Row(row.getString(1), row.getString(2)))
    )

    // drop nulls
    checkAnswer(
      Melt.of(meltWideDataDs.select($"str1", $"str2"),
        Seq.empty, Seq("str1", "str2"), dropNulls = true),
      meltedWideDataRows.filter(row => !row.isNullAt(2))
        .map(row => Row(row.getString(1), row.getString(2)))
    )
  }

  test("melt with single id") {
    // do not drop nulls
    checkAnswer(
      Melt.of(meltWideDataDs, Seq("id"), Seq("str1", "str2")),
      meltedWideDataRows
    )

    // drop nulls
    checkAnswer(
      Melt.of(meltWideDataDs, Seq("id"), Seq("str1", "str2"), dropNulls = true),
      meltedWideDataRows.filter(row => !row.isNullAt(2))
    )

    // with id column `variable`
    assertException[IllegalArgumentException] {
      Melt.of(
        meltWideDataDs.withColumnRenamed("id", "variable"),
        Seq("variable"), Seq("str1", "str2")
      )
    }("Column name for variable column (variable) must not exist among id columns: variable")
    checkAnswer(
      Melt.of(meltWideDataDs.withColumnRenamed("id", "variable"),
        Seq("variable"), Seq("str1", "str2"), variableColumnName = "var", valueColumnName = "val"),
      meltedWideDataRows
    )

    // with id column `value`
    assertException[IllegalArgumentException] {
      Melt.of(meltWideDataDs.withColumnRenamed("id", "value"),
        Seq("value"), Seq("str1", "str2"))
    }("Column name for value column (value) must not exist among id columns: value")
    checkAnswer(
      Melt.of(meltWideDataDs.withColumnRenamed("id", "value"),
        Seq("value"), Seq("str1", "str2"), variableColumnName = "var", valueColumnName = "val"),
      meltedWideDataRows
    )

    // with value column `variable` and `value`
    checkAnswer(
      Melt.of(meltWideDataDs.withColumnRenamed("str1", "variable")
        .withColumnRenamed("str2", "value"),
        Seq("id"), Seq("variable", "value")),
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
      Melt.of(meltWideDataDs.withColumnRenamed("int1", "variable")
        .withColumnRenamed("long1", "value"),
        Seq("id"), Seq("str1", "str2")),
      meltedWideDataRows
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
      Melt.of(meltWideDataDs, Seq("id", "int1"), Seq("str1", "str2")),
      meltedRows
    )

    // drop nulls
    checkAnswer(
      Melt.of(meltWideDataDs, Seq("id", "int1"), Seq("str1", "str2"), dropNulls = true),
      meltedRows.filter(row => !row.isNullAt(3))
    )
  }

  test("melt without values") {
    // do not drop nulls
    checkAnswer(
      Melt.of(meltWideDataDs.select($"id", $"str1", $"str2"), Seq("id")),
      meltedWideDataRows
    )

    // do drop nulls
    checkAnswer(
      Melt.of(meltWideDataDs.select($"id", $"str1", $"str2"), Seq("id"), dropNulls = true),
      meltedWideDataRows.filter(row => !row.isNullAt(2))
    )
  }

  test("melt with incompatible value types") {
    assertException[IllegalArgumentException] {
      Melt.of(meltWideDataDs, Seq("id"), Seq("str1", "int1"))
    }("All values must be of same types, found: IntegerType, StringType")
  }

  /** TODO: would be nice if LongType and IntegerType columns could be used together.
  test("melt with compatible value types") {
    val df = Melt.of(meltWideDataDs, Seq("id"), Seq("int1", "long1"))

    assert(df.schema === StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("variable", StringType, nullable = false),
      StructField("value", LongType, nullable = true),
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

    // do not drop nulls
    checkAnswer(
      df,
      meltedRows
    )

    // drop nulls
    checkAnswer(
      Melt.of(meltWideDataDs, Seq("id"), Seq("int1", "long1"), dropNulls = true),
      meltedRows.filter(row => !row.isNullAt(2))
    )
  } */

  test("melt with invalid arguments") {
    // melting with empty list of value columns
    assertException[IllegalArgumentException] {
      Melt.of(meltWideDataDs, Seq.empty, Seq.empty)
    }("All values must be of same types, found: IntegerType, LongType, StringType")
    assertException[IllegalArgumentException] {
      Melt.of(meltWideDataDs, Seq("id"), Seq.empty)
    }("All values must be of same types, found: IntegerType, LongType, StringType")

    // melting without giving values and no non-id columns
    assertException[IllegalArgumentException] {
      Melt.of(meltWideDataDs.select("id"), Seq("id"))
    }("The dataset has no non-id columns to melt")
  }

  test("melt with dot and backtick") {
    val df = meltWideDataDs
      .withColumnRenamed("id", "`an.id`")
      .withColumnRenamed("str1", "`str.one`")
      .withColumnRenamed("str2", "`str.two`")
    checkAnswer(
      Melt.of(df, Seq("`an.id`"), Seq("`str.one`", "`str.two`")),
      meltedWideDataRows.map(row => Row(
        row.getInt(0),
        row.getString(1) match {
          case "str1" => "str.one"
          case "str2" => "str.two"
        },
        row.getString(2)
      ))
    )
  }

  test("melt with struct fields") {
    val df = meltWideDataDs.select(
      struct($"id").as("an"),
      struct(
        $"str1".as("one"),
        $"str2".as("two")
      ).as("str")
    )
    checkAnswer(
      Melt.of(df, Seq("an.id"), Seq("str.one", "str.two")),
      meltedWideDataRows.map(row => Row(
        row.getInt(0),
        row.getString(1) match {
          case "str1" => "str.one"
          case "str2" => "str.two"
        },
        row.getString(2)
      ))
    )
  }
}
