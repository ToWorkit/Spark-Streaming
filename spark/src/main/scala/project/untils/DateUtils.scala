package project.untils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  *
  */
object DateUtils {
  // 2018-06-24 12:11:01 日志格式
  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  // 输出格式
  val TARGE_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  /**
    * 获取时间戳
    * @param time
    * @return
    */
  def getTime(time: String) = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  /**
    * 解析成输出格式
    * @param time
    * @return
    */
  def parseToMinute(time :String) = {
    TARGE_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parseToMinute("2018-06-24 12:11:01"))
  }
}
