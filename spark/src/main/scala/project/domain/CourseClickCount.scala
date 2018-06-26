package project.domain

/**
  * 课程点击数
  * @param day_course 对应的就是HBase中的Rowkey 20181111_1
  * @param click_count 对应的就是20181111_1的访问总数
  */
case class CourseClickCount(day_course: String, click_count: Long)
