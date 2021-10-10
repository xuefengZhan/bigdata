package com.yato.bigdata.scala.chapter05

/**
 * yatolovefantasy
 * 2021-08-31-23:14
 */
object Scala05_Function_Hell {
  def main(args: Array[String]): Unit = {
    //todo 1.将函数作为值赋给变量
    def test() : Unit = {
      println("f1....")
    }

    // 问题1：尝试将函数对象test赋值给变量f，然后通过f来执行函数
    //val f = test
    //f()
    // 执行报错

    // 原因：
    // 知识点1.如果函数声明的参数列表没有参数，在调用的时候可以省略小括号，
    // 因此下面两种写法是等价的
    // val f = test
    // val f = test()

    // 由于f是方法的执行结果，因此f就不是个函数，无法f()

    // 解决方案：
    // 使用特殊符号来声明，将函数作为一个对象赋值给变量，而不是让这个函数执行
    val f = test _
    f()

    val f1 : ()=> Unit = test
    f1()
  }
}
