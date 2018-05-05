package com.bigdata.util

import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.sql._

import scala.collection.mutable.StringBuilder

object JsonFormat {

  /**
    *
    * @param data
    * @return
    *         {
    *           "data": [
    *             {
    *               "val": "name",
    *               "count": 10
    *             }
    *           ]
    *         }
    */
  def format(data: Array[Row]): String = {
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
        appendCount += 1
      }
    })
    if (appendCount > 0) {
      // remove last comma
      builder.deleteCharAt(builder.length - 1)
    }
    builder.append("]}")

    builder.toString()
  }

  def formatNumberArray(array: Array[Row]): String = {
    val builder = StringBuilder.newBuilder
    builder.append("{")
    builder.append("\"data\":[")
    var appendCount = 0
    array.foreach(row => {
      var shouldAppend = true
      var var1 = 0d
      var var2 = 0d
      try {
        var1 = row(0).toString.toFloat
        var2 = row(1).toString.toFloat
      } catch {
        case _: Throwable => {
          shouldAppend = false
        }
      }
      if (shouldAppend) {
        appendCount = appendCount + 1
        builder.append("[")
        builder.append(var1)
        builder.append(",")
        builder.append(var2)
        builder.append("],")
      }
    })
    if (appendCount > 0) {
      builder.deleteCharAt(builder.length - 1)
    }
    builder.append("]}")
    builder.toString()
  }
}
