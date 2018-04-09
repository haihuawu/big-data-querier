package com.bigdata.service

import com.bigdata.spark.SparkFactory
import com.bigdata.util.{AppConfig, Cache, Util}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.format_string

import scala.collection.mutable.StringBuilder
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.sql.Row
import org.apache.commons.lang.StringEscapeUtils


// if you don't supply your own Protocol (see below)

object SingleProfile {

  val singleProfile = "single-profile"

  def getSingleProfile(table: String, column: String): String = {
    val  key = Util.concat(singleProfile, table, column)
    if (Cache.hasKey(key)) {
      return Cache.getFromCache(key)
    }
    val path = AppConfig.hfsBasePath + table + ".csv"
    val file = SparkFactory.spark.read.format("csv").option("header", "true").load(path)
    val data = file.groupBy(col(column)).count().collect()
    val result = jsonFormat(data)
    Cache.putInCache(key, result)
    return result
  }

  /**
    *
    * @param data
    * @return JSON String
    *         {
    *           "data": [{"val": "abc", "count": number}]
    *         }
    */
  def jsonFormat(data: Array[Row]): String = {
    val builder = StringBuilder.newBuilder
    builder.append("{")
    builder.append("\"data\":[")
    var appendCount = 0
    data.foreach(row => {
      var value = ""
      var count = "0"
      var shouldAppend = true
      try {
        value = StringEscapeUtils.escapeJava(row.get(0).toString)
        count = row.get(1).toString
      } catch {
        case _: Throwable => {
          shouldAppend = false
        }
      }
      if (shouldAppend) {
        builder.append("{\"val\":\"")
        builder.append(value)
        builder.append("\", \"count\":")
        builder.append(row.get(1))
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
