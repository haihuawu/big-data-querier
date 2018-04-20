package com.bigdata.service

import com.bigdata.spark.SparkFactory
import com.bigdata.util.{AppConfig, JsonFormat, Util, Cache}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer

object CommonValue {

  val commonValue = "common-value"

  def getCommonValues(tables: String, columns: String): String = {
    val key = getKey(tables, columns)
    if (Cache.hasKey(key)) {
      return Cache.getFromCache(key)
    }
    val tableArray = getArray(tables)
    val columnArray = getArray(columns)
    val dfs = getDataFrames(tableArray, columnArray)
    var i = 0
    var joinedDataFrame = dfs(0).withColumn("joined_column", col(columnArray(0))).select("joined_column")
    for (i <- 1 until dfs.length) {
      joinedDataFrame = joinedDataFrame.join(dfs(i), joinedDataFrame("joined_column") === dfs(i)(columnArray(i)), "inner").select("joined_column")
    }
    println(joinedDataFrame.count())
    joinedDataFrame = joinedDataFrame.groupBy("joined_column").count()
    val result = JsonFormat.format(joinedDataFrame.collect())
    Cache.putInCache(key, result)
    return result
  }

  def getKey(tables: String, columns: String): String = {
    Util.concat(commonValue, "tables", tables, "columns", columns)
  }

  private def getArray(commaSeparatedString: String): Array[String] = {
    commaSeparatedString.split(",")
  }

  private def getDataFrameByTable(table: String): DataFrame = {
    val path = AppConfig.hfsBasePath + table + ".csv"
    val file = SparkFactory.spark.read.format("csv").option("header", "true").load(path)
    return file
  }

  private def getDataFrames(tables: Array[String], columns: Array[String]): List[DataFrame] = {
    val buffer = ListBuffer[DataFrame]()
    var i = 0
    for (i <- tables.indices) {
      var df = getDataFrameByTable(tables(i))
      df = df.select(columns(i))
      buffer += df
    }
    return buffer.toList
  }


}
