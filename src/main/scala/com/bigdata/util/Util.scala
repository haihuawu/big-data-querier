package com.bigdata.util

object Util {

  def concat(pieces: String*): String = {
    val result = StringBuilder.newBuilder
    pieces.foreach(piece => {
      result.append(piece)
      result.append(',')
    })
    if (pieces.nonEmpty) {
      // remove last comma
      result.deleteCharAt(result.length - 1)
    }
    return result.toString()
  }

}
