package com.spoddutur.service

import com.spoddutur.spark.SparkFactory
import com.spoddutur.util.AppConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.format_string
import scala.collection.mutable.StringBuilder
import spray.json._
import DefaultJsonProtocol._ // if you don't supply your own Protocol (see below)

object SingleProfile {
  /**
    *
    * @param table
    * @param column
    * @return
    */
  def getSingleProfile(table: String, column: String): String = {
    /**
      * TODO:
      * file can be pre-loaded to improve performance
      * */
    val path = AppConfig.hfsBasePath + table + ".csv"
    val file = SparkFactory.spark.read.format("csv").option("header", "true").load(path)
    val data = file.groupBy(col(column)).count().select(format_string("%s\t%d", col(column), col("count"))).collect()
    // val data = Array("a", "b", "c")
    val result = StringBuilder.newBuilder
    data.foreach(line => {
      result.append("<p>")
      result.append(line)
      result.append("</p>")
    })
    return result.toString()
  }

}
