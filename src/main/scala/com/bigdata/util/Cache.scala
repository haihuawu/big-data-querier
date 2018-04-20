package com.bigdata.util

object Cache {

  val map = scala.collection.mutable.Map[String, String]()

  private val MAX_SIZE = 10000

  def putInCache(key: String, value: String) = {
    if (map.size > MAX_SIZE) {
      map.clear()
    }
    map.put(key, value)
  }

  def getFromCache(key: String): String = {
    map(key)
  }

  def hasKey(key: String): Boolean = {
    return map.contains(key)
  }

}
