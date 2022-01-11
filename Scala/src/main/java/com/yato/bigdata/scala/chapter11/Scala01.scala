package com.yato.bigdata.scala.chapter11

/**
 * yatolovefantasy
 * 2021-09-11-16:38
 */
object Scala01 {
  def main(args: Array[String]): Unit = {
    //todo 1.
    val ttt:TTT[B] = new TTT[B]()
    //todo 2.泛型协变
    // scala希望将泛型和类型当作一个整体来使用
    // 这个整体的类型和存在上下级关系
    // TTT[父类] 就是TTT[子类]的 父类
    // 叫做泛型的协变，在反省前添加特殊符号 +
    val ttt3 : TTT[B] = new TTT[C]()

    //todo 3.泛型逆变
    val yyy3 : yyy[B] = new yyy[A]()




  }
}

class TTT[+T]{}

class yyy[-T]{}

class A{}

class B extends A{}

class C extends B{}