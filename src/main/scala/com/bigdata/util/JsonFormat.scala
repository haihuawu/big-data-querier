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

    return builder.toString()
    // return "hello..."
  }
}
