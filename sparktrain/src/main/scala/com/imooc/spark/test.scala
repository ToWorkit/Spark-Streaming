package com.imooc.spark

/*
构造器：（1）主构造器  : 和类的申明在一起，只能有一个主构造器
       （2）辅助构造器:  多个辅助构造器，通过关键字：this
 */
class Student(val stuName: String, val stuAge: Int) {
  // 定义一个辅助构造器
  def this(age: Int) {
    // 调用主构造器
    this("No Name", age)
  }
}


object Student {
  def main(args: Array[String]): Unit = {
    // 使用主构造器创建学生对象
    var s = new Student("Tom", 21)
    println(s.stuName + "\t" + s.stuAge)

    // 使用构造器创建学生对象
    var s1 = new Student(24)
    println(s1.stuName + "\t" + s1.stuAge)
  }
}
