package com.bigdata.service

import com.bigdata.spark.SparkFactory
import com.bigdata.util.{AppConfig, Cache, JsonFormat, Util}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.mllib.stat.Statistics

object CrossTableCorrelation {

  private val crossTableCorrelation = "cross-table-correlation"

  private val limit = 1000

  def getCorrelationArray(table1: String, table2: String, column1: String, column2: String, join: String): String = {
    val key = getKey(table1, table2, column1, column2)
    if (Cache.hasKey(key)) {
      return Cache.getFromCache(key)
    }
    var result = ""
    val rdd
    if (table1.equals(table2)) {
      val df = getDataFrameByTable(table1)
      rdd = df.select(column1, column2).filter(col(column1) =!= 0).filter(col(column2) =!= 0).rdd
    } else {
      val df1 = getDataFrameByTable(table1).select(column1, join).filter(col(column1) =!= 0)
      val df2 = getDataFrameByTable(table2).select(column2, join).filter(col(column2) =!= 0)
      rdd = df1.join(df2, df1(join) === df2(join), "inner").rdd
    }
    val array = rdd.takeSample(false, limit)
    result = JsonFormat.formatNumberArray(array)

    val seriesX: RDD[Double] = sc.parallelize()

    val Double = Statistics.corr(seriesX, seriesY, "pearson")

    Cache.putInCache(key, result)
    result
  }

  def getKey(table1: String, table2: String, column1: String, column2: String): String = {
    Util.concat(crossTableCorrelation,  table1, table2, column1, column2)
  }

  private def getDataFrameByTable(table: String): DataFrame = {
    val path = AppConfig.hfsBasePath + table + ".csv"
    val df = SparkFactory.spark.read.format("csv").option("header", "true").load(path)
    df
  }



}
