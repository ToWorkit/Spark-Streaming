package project.dao

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import project.domain.CourseClickCount
import project.utils.HBaseUtils

import scala.collection.mutable.ListBuffer

/**
  * 课程点击数数据访问层
  */
object CourseClickCountDAO {
  val tableName = "imooc_course_clickcount"
  // 可以放多个列
  val cf = "info"
  // 列名
  val qualifer = "click_count"

  /**
    * 保存数据到HBase
    * @param list CourseClickCount集合
    */
  def save(list: ListBuffer[CourseClickCount]): Unit = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
  }

  /**
    * 根据rowkey查询值
    * @param day_course
    */
  def count(day_course: String) = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }
}
