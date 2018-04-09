package com.bigdata.util

object Cache {

  val map = scala.collection.mutable.Map[String, String]()

  def putInCache(key: String, value: String) = {
    map.put(key, value)
  }

  def getFromCache(key: String): String = {
    map(key)
  }

  def hasKey(key: String): Boolean = {
    return map.contains(key)
  }

}
