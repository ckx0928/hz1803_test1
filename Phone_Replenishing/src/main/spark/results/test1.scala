package results

import java.text.SimpleDateFormat
import java.util.Locale

object test1 {
  def main(args: Array[String]): Unit = {
    //val loc = new Locale("en")
    val fm = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val tm = "20170412030007003"
    val dt2 = fm.parse(tm).getTime;
    print(dt2)
  }
}
