package com.bigdata.service

import org.apache.spark.sql.DataFrame
import com.bigdata.util.{AppConfig, Cache, Util}
import com.bigdata.spark.SparkFactory
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.Vectors

object CosineSimilarity {

  val cosineSimilarity = "cosine-similarity"

  def cosineSimilarity(table: String, columna : String, columnb : String): String = {
    val key = getKey(table, columna, columnb)
    if (Cache.hasKey(key)) {
      return Cache.getFromCache(key)
    }
    val dfa = getDataFrameByTable(table).select(columna, columnb)
    var rdd = dfa.rdd
    rdd = rdd.filter(line => line(0) != null && line(1) != null)
    rdd = rdd.filter(line => line(0).toString() forall Character.isDigit)
    rdd = rdd.filter(line => line(1).toString() forall Character.isDigit)

    val rowsVector = rdd.map{
        x =>
          Vectors.dense(
            x(0).toString().toDouble,
            x(1).toString().toDouble)
    }

    val mat = new RowMatrix(rowsVector)

    val sim = mat.columnSimilarities();
    //sim.entries.collect()

    val result = jsonFormat(sim)
    Cache.putInCache(key, result)
    return result
  }

  def getKey(table: String, columna: String, columnb: String): String = {
    Util.concat(cosineSimilarity,  table, columna, columnb)
  }

  def getDataFrameByTable(table: String): DataFrame = {
    val path = AppConfig.hfsBasePath + table + ".csv"
    val file = SparkFactory.spark.read.format("csv").option("header", "true").load(path)
    return file
  }

  def jsonFormat(columna : String, columnb : String, simi : CoordinateMatrix): String = {
    val similist = simi.entries.collect()

    val json = StringBuilder.newBuilder

    for (s <- similist) {
      json.append("{")
      json.append("\"")
      json.append(columna)
      json.append("\" : ")
      json.append(s.i.toString)
      json.append(",")
      json.append("\"")
      json.append(columnb)
      json.append("\" : ")
      json.append(s.j.toString)
      json.append(",")
      json.append("\"val\" : ")
      json.append(s.value.toString)
      json.append("}")
    }

    return json.toString()
  }

}