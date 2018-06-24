package project.domain

/**
  * 清洗后的日志信息(数据类型)
  * @param ip 日志访问的ip地址
  * @param time 日志访问的时间
  * @param courseId 课程id
  * @param statusCode 访问状态码
  * @param referer 从哪来的
  */
case class ClickLog(ip: String, time: String, courseId: Int, statusCode: Int, referer: String)

