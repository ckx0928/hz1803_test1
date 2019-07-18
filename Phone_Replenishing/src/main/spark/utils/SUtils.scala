package utils

import java.text.SimpleDateFormat

class SUtils {
  def parseTimestamp(time:String): Long ={
    val fm = new SimpleDateFormat("yyyyMMddHHmmss")
    return fm.parse(time).getTime;
  }
}
