package com.yato.bigdata.scala.chapter05

/**
 * yatolovefantasy
 * 2021-09-02-0:03
 */
object Scala06_Function {
  def main(args: Array[String]): Unit = {
    //todo 控制抽象语法：
    // 原本的写法： fun7( f: () => Unit) = {...}
    // 控制抽象写法： 参数中函数对象类型 ：不写参数部分
    //             参数中函数对象形参 ： 不写，用op代替
    def fun7(op: => Unit) = {
      op  // op 等同于调用这个函数    执行参数中的逻辑
    }

    // todo 调用
    // 将逻辑作为参数传递进来了
    // 并且，由于逻辑是多行，所以可以用花括号代替小括号

    fun7{
      val i = 10
      val j = 20
      print(i+j)
    }

    // 不需要函数的名称，不需要函数的声明，只要逻辑！

  }
}
