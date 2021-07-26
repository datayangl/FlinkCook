package com.code.ly.flink.stream.test

class MatchDemo {


}

object MatchDemo{
  def main(args: Array[String]): Unit = {
    val stu = Student(1, "jack")

    stu match {
      case Student(a,b) => {
        print(a+b)
      }
    }
  }
}
case class Student(id:Int, name:String)