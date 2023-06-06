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

package org.apache.spark.sql.jdbc

import java.sql.Statement

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

trait UpsertByTempTable {
  self: JdbcDialect =>

  /**
   * Create a temporary table with the schema of an existing table. If `keyColumns` are provided,
   * a primary index will be added for the temporary table.
   *
   * Table name `existingTableName` and columns in `columns`
   * are expected to be quoted by `JdbcDialect.quoteIdentifier`.
   *
   * @param existingTableName
   * @param keyColumns
   * @param options
   */
  def getCreateTempTableFromTableQuery(
      existingTableName: String,
      keyColumns: Array[String],
      options: JDBCOptions): (String, String)

  def createTempTableFromTable(
      stmt: Statement,
      existingTableName: String,
      keyColumns: Array[String],
      options: JDBCOptions): String = {
    val (tempTable, sql) = getCreateTempTableFromTableQuery(existingTableName, keyColumns, options)
    stmt.executeUpdate(sql)
    tempTable
  }

  /**
   * Returns a SQL query that updates all rows from `sourceTableName` that exist
   * in `destinationTableName` w.r.t. to the `keyColumns`. Other rows are ignored.
   * Also see `getInsertTableFromTableQuery`.
   *
   * Table names `destinationTableName` and `sourceTableName`, as well as columns in `columns`
   * are expected to be quoted by `JdbcDialect.quoteIdentifier`.
   *
   * @param destinationTableName
   * @param sourceTableName
   * @param columns
   * @param keyColumns
   * @return sql query
   */
  def getUpdateTableFromTableQuery(
      destinationTableName: String,
      sourceTableName: String,
      columns: Array[String],
      keyColumns: Array[String]): String

  /**
   * Updates all rows from `sourceTableName` that exist in `destinationTableName` w.r.t. to
   * the `keyColumns`. Other rows are ignored. Also see `insertTableFromTable`.
   *
   * Table names `destinationTableName` and `sourceTableName`, as well as columns in `columns`
   * are expected to be quoted by `JdbcDialect.quoteIdentifier`.
   *
   * @param stmt
   * @param destinationTableName
   * @param sourceTableName
   * @param columns
   * @param keyColumns
   * @return number of updated rows
   */
  def updateTableFromTable(
      stmt: Statement,
      destinationTableName: String,
      sourceTableName: String,
      columns: Array[String],
      keyColumns: Array[String]): Int = {
    val sql1 =
      """
        |SELECT DB_NAME(resource_database_id) AS dbname,
        |       OBJECT_NAME(resource_associated_entity_id) AS ObjectName,
        |       *
        |FROM sys.dm_tran_locks
        |WHERE dm_tran_locks.resource_type = 'OBJECT' AND request_status != 'GRANT'
        |""".stripMargin
    val res =
      stmt.getConnection.createStatement().executeQuery(sql1)
    val meta = res.getMetaData
    val cols = 1.to(meta.getColumnCount).map(meta.getColumnName)
    Console.println(cols.mkString(", "))
    while (res.next()) {
      val row = 1.to(meta.getColumnCount)
        .map(res.getString)
        .map(v => Option(v).map(_.trim).orNull)
      Console.println(row)
    }
    res.close()


    val sql = getUpdateTableFromTableQuery(destinationTableName, sourceTableName,
      columns, keyColumns)
    try {
      stmt.executeUpdate(sql)
    } catch {
      case e: Exception =>
        val sql2 =
          """
            |SELECT *
            |FROM sys.dm_tran_locks
            |WHERE resource_type = 'OBJECT' AND request_status != 'GRANT'
            |""".stripMargin
        val res =
          stmt.executeQuery(sql2)
        val meta = res.getMetaData
        val columns = 1.to(meta.getColumnCount).map(meta.getColumnName)
        Console.println(columns.mkString(", "))
        while (res.next()) {
          val row = 1.to(meta.getColumnCount)
            .map(res.getString)
            .map(v => Option(v).map(_.trim).orNull)
          Console.println(row)
        }
        res.close()

        throw e
    }
  }

  /**
   * Returns a SQL query that inserts all rows from `sourceTableName` that do not exist
   * in `destinationTableName` w.r.t. to the `keyColumns`. Other rows are ignored.
   * Also see `getUpdateTableFromTableQuery`.
   *
   * Table names `destinationTableName` and `sourceTableName`, as well as columns in `columns`
   * are expected to be quoted by `JdbcDialect.quoteIdentifier`.
   *
   * @param destinationTableName
   * @param sourceTableName
   * @param columns
   * @param keyColumns
   * @return sql query
   */
  def getInsertTableFromTableQuery(
      destinationTableName: String,
      sourceTableName: String,
      columns: Array[String],
      keyColumns: Array[String]): String

  /**
   * Inserts all rows from `sourceTableName` that do not exist in `destinationTableName` w.r.t. to
   * the `keyColumns`. Other rows are ignored. Also see `updateTableFromTableQuery`.
   *
   * Table names `destinationTableName` and `sourceTableName`, as well as columns in `columns`
   * are expected to be quoted by `JdbcDialect.quoteIdentifier`.
   *
   * @param stmt
   * @param destinationTableName
   * @param sourceTableName
   * @param columns
   * @param keyColumns
   * @return number of inserted rows
   */
  def insertTableFromTable(
      stmt: Statement,
      destinationTableName: String,
      sourceTableName: String,
      columns: Array[String],
      keyColumns: Array[String]): Int = {
    val sql = getInsertTableFromTableQuery(destinationTableName, sourceTableName,
      columns, keyColumns)
    stmt.executeUpdate(sql)
  }

}
