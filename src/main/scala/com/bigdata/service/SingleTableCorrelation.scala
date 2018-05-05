package com.bigdata.service

import org.apache.spark.sql.DataFrame
import com.bigdata.util.{AppConfig, Cache, Util}
import com.bigdata.spark.SparkFactory
import org.apache.commons.lang.StringEscapeUtils
import scala.collection.mutable.{ListBuffer, StringBuilder}

object GetCorrelation {

  val singleTableCorrelation = "singletable-correlation"

  def singleTableCorrelation(table: String, columna : String, columnb : String): String = {
    val key = getKey(table, columna, columnb)
    if (Cache.hasKey(key)) {
      return Cache.getFromCache(key)
    }

    var df = getDataFrameByTable(table).select(columna, columnb)

    var rdd = df.rdd
    rdd = rdd.filter(line => line(0) != null && line(1) != null)
    rdd = rdd.filter(item => item(0).toString() forall Character.isDigit)
    rdd = rdd.filter(item => item(1).toString() forall Character.isDigit)

    rdd = rdd.sortBy(item => item(0).toString(), true)
    rdd = rdd.sortBy(item => item(1).toString(), true)

    val rddrevese = rdd.sortBy(item => item(0).toString(), false)
 
    val min = rdd.first()(0).toString().toInt
    val max = rddrevese.first()(0).toString.toInt

    //generate output list, factor : 20
    val valueList = getValueList(min, max, 20)
    val result = ListBuffer[(Int, Int)]()


    valueList.foreach(value => {
      rdd = rdd.filter(item => item(0).toString().toInt >= value)
      result += Tuple2(rdd.first()(0).toString().toInt, rdd.first()(1).toString().toInt)
      })



    val outputList = result.toList



    val result = jsonFormat(similarity)
    Cache.putInCache(key, result)
    return result
  }

  def getValueList(min: Int, max: Int, factor: Int): List[Int] = {
    val result = ListBuffer[Int]()
    val gap = max/factor
    var tempVal = min
    while (tempVal <= max) {
      result += tempVal
      tempVal = tempVal + gap
    }
    return result.toList
  }


  def getKey(table: String, columna: String, columnb: String): String = {
    Util.concat(singleTableCorrelation, table, columna, columnb)
  }

  def getDataFrameByTable(table: String): DataFrame = {
    val path = AppConfig.hfsBasePath + table + ".csv"
    val file = SparkFactory.spark.read.format("csv").option("header", "true").load(path)
    return file
  }

  def jsonFormat(simi : Float): String = {
    val json = StringBuilder.newBuilder
    json.append("{")
    json.append("\"val\" : ")
    json.append(simi.toString())
    json.append("}")
    return json.toString()
  }

}
