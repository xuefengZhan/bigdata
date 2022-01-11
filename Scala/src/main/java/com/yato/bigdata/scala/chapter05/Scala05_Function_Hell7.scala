package com.yato.bigdata.scala.chapter05

/**
 * yatolovefantasy
 * 2021-09-01-1:56
 *
 * day3. 6闭包
 */
object Scala05_Function_Hell7 {
  def main(args: Array[String]): Unit = {
    def test() : Unit = {
      print("xxx")
    }

    // todo 将函数对象传递给变量，也有闭包
    //  a 可以做返回值 将函数对象传递给作用域外部的地方使用  局部功能放到外部使用，
    //  从而更改了声明周期
    val a = test _
  }
}
