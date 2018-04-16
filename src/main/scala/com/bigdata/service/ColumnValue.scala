package com.bigdata.service

import com.bigdata.spark.SparkFactory
import com.bigdata.util.{AppConfig, Cache, Util}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.commons.lang.StringEscapeUtils

import scala.collection.mutable.{ListBuffer, StringBuilder}

object ColumnValue {

  val columnValue = "column-value"

  def getColumns(table: String, hasVal: String, notHasVal: String): String = {
    val key = getKey(table, hasVal, notHasVal)
    if (Cache.hasKey(key)) {
      return Cache.getFromCache(key)
    }
    val df = getDataFrameByTable(table)
    val hasValColumns = getHasList(df, hasVal, arrayToList(df.columns))
    val notHasValColumns = getNotHasList(df, notHasVal, hasValColumns)
    val result = jsonFormat(notHasValColumns)
    Cache.putInCache(key, result)
    return result
  }

  def getKey(table: String, hasVal: String, notHasVal: String): String = {
    Util.concat(columnValue, "hasVal", hasVal, "notHasVal", notHasVal)
  }

  def getDataFrameByTable(table: String): DataFrame = {
    val path = AppConfig.hfsBasePath + table + ".csv"
    val file = SparkFactory.spark.read.format("csv").option("header", "true").load(path)
    return file
  }

  def getValueList(values: String): List[String] = {
    val result = ListBuffer[String]()
    val pieces = values.split(",")
    pieces.foreach(piece => {
      result += piece
    })
    return result.toList
  }

  def getHasList(df: DataFrame, value: String, columns: List[String]): List[Tuple2[String, Long]] = {
    val result = ListBuffer[Tuple2[String, Long]]()
    val list = getValueList(value)
    columns.foreach(column => {
      val filtered = df.filter(col(column).isin(list: _*))
      if (filtered.count() > 0) result += Tuple2(column, filtered.count())
    })
    return result.toList
  }

  def getNotHasList(df: DataFrame, value: String, columns: List[(String, Long)]): List[Tuple2[String, Long]] = {
    val result = ListBuffer[Tuple2[String, Long]]()
    val list = getValueList(value)
    if (list.isEmpty) {
      return columns
    }
    columns.foreach(column => {
      val filtered = df.filter(col(column._1).isin(list: _*))
      if (filtered.count() == 0) result += column
    })
    return result.toList
  }

  def arrayToList(arr: Array[String]): List[String] = {
    val result = ListBuffer[String]()
    arr.foreach(arrVal => {
      result += arrVal
    })
    return result.toList
  }

  def jsonFormat(ls: List[Tuple2[String, Long]]): String = {
    val builder = StringBuilder.newBuilder
    builder.append("{")
    builder.append("\"data\":[")
    var appendCount = 0
    ls.foreach(row => {
      var value = ""
      var count = 0L
      var shouldAppend = true
      try {
        value = StringEscapeUtils.escapeJava(row._1)
        count = row._2
      } catch {
        case _: Throwable => {
          shouldAppend = false
        }
      }
      if (shouldAppend) {
        builder.append("{\"val\":\"")
        builder.append(value)
        builder.append("\", \"count\":")
        builder.append(count)
        builder.append("}")
        builder.append(",")
        appendCount+=1
      }
    })
    if (appendCount > 0) {
      // remove last comma
      builder.deleteCharAt(builder.length - 1)
    }
    builder.append("]}")

    return builder.toString()
    // return "hello..."
  }

}