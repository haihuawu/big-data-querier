package com.bigdata.service

import org.apache.spark.sql.DataFrame
import com.bigdata.util.{AppConfig, Cache, Util}
import com.bigdata.spark.SparkFactory
import org.apache.commons.lang.StringEscapeUtils

/**
  * Yo huahua:
  *
  * compare two columns with Jaccard Similarity
  *
  * sim(c1, c2) = |c1 n c2| / |c1 u c2|
  *
  * how to start:
  *
  * 1. set up http get incoming request in com.bigdata.web.WebService
  * 2. finish the below class, create an getSimilarity() method that returns a value between 0-1 that represents the similarity
  * look at ColumnValue, SingleProfile for some coding example
  * */
object ColumnSimilarity {

  def computeSimilarity(tablea: String, tableb: String, columna : String, columnb : String): String = {

    val computeSimilarity = "compute-similarity"

    val dfa = getDataFrameByTable(tablea).select(columna)
    val dfb = getDataFrameByTable(tableb).select(columnb)

    val rdda = dfa.rdd
    val rddb = dfb.rdd

    val mapc1 = rdda.map(s => (s, 1)).groupByKey().map({case(k,v) => (k, ("C1", v.size))})
    val mapc2 = rddb.map(s => (s, 1)).groupByKey().map({case(k,v) => (k, ("C2", v.size))})

    val map_union = mapc1.union(mapc2).groupByKey()

    val total_count = map_union.count()

    map_union.filter({case(k,v) => v.size == 2})

    val similar_count = map_union.count()

    val similarity : Float = similar_count.toFloat/total_count.toFloat
    val result = jsonFormat(similarity)

    return result
  }

  def getDataFrameByTable(table: String): DataFrame = {
    val path = AppConfig.hfsBasePath + table + ".csv"
    val file = SparkFactory.spark.read.format("csv").option("header", "true").load(path)
    return file
  }

  def jsonFormat(simi : Float): String = {
    val json = StringBuilder.newBuilder
    json.append("[")
    json.append("Similarity : ")
    json.append(simi.toString())
    json.append("]")
    return json.toString()
  }

}