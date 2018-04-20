package com.bigdata.service

import com.bigdata.spark.SparkFactory
import com.bigdata.util.{AppConfig, Cache, Util, JsonFormat}
import org.apache.spark.sql._
import org.apache.commons.lang.StringEscapeUtils

object TableColumns {

  val tableColumns = "table-columns"

  def getTableColumns(table: String): String = {
    val key = getKey(table)
    if (Cache.hasKey(key)) {
      return Cache.getFromCache(key)
    }
    val df = getDataFrameByTable(table)
    val result = jsonFormat(df.columns)
    Cache.putInCache(key, result)
    return result
  }

  def getDataFrameByTable(table: String): DataFrame = {
    val path = AppConfig.hfsBasePath + table + ".csv"
    val file = SparkFactory.spark.read.format("csv").option("header", "true").load(path)
    return file
  }

  def getKey(table_name: String): String = {
    return Util.concat(tableColumns, table_name)
  }

  def jsonFormat(columns: Array[String]): String = {
    val json = StringBuilder.newBuilder
    json.append("{")
    json.append("\"data\":[")
    columns.foreach(column => {
      json.append("\"")
      json.append(StringEscapeUtils.escapeJava(column))
      json.append("\",")
    })
    if (columns.nonEmpty) {
      json.deleteCharAt(json.length - 1)
    }
    json.append("]}")
    return json.toString()
  }

}
