package com.spoddutur.spark

import com.spoddutur.util.AppConfig
import org.apache.spark.sql.SparkSession

/**
  * Created by sruthi on 03/07/17.
  * Creates one SparkSession which is shared and reused among multiple HttpRequests
  */
object SparkFactory {
  val spark: SparkSession = SparkSession.builder
    .master(AppConfig.sparkMaster)
    .appName(AppConfig.sparkAppName)
    .getOrCreate

  val sc = spark.sparkContext
  val sparkConf = sc.getConf

  /**
    * in an ideal world, memory resource is unlimited so we can load all the tables in memory
    *
    * now, pre-load the movie dataset for testing purpose
    * */

  // var rdds:Map[String, RDD] = Map()
  // val moviesPath = AppConfig.hfsBasePath + "movies_metadata.csv"
  // val movies = spark.read.format("csv").option("header", "true").load(moviesPath)

}
