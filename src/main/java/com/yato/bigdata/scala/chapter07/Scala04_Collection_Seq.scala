package com.yato.bigdata.scala.chapter07

/**
 * yatolovefantasy
 * 2021-09-08-1:22
 */
object Scala04_Collection_Seq {
  def main(args: Array[String]): Unit = {
   //TODO 集合 Seq
    //Scala默认提供的集合是不可变的，也就是imutable包下的集合
    //List 是抽象类，但是有伴生对象
    val list = List(1, 2, 3, 4)
    val list1 = list :+ 5
    val list2 = 5 +: list
    println(list1)
    println(list2)


    //todo 2.空集合
    //Scala中采用特殊对象Nil 代表空集合
    println(List()) //List()
    //3::Nil 等价于List(3)
    Nil
    val list3 = 1::2::3::Nil
    println(list3) //List(1, 2, 3)
    println(Nil) //List()

    //::: 将集合拆成元素来添加
    //:: 将对象当作整体来添加
    val list4 = 4::5::list3:::Nil
    val list5 = 4::5::list3::Nil
    println(list4) //List(4, 5, 1, 2, 3)
    println(list5) //List(4, 5, List(1, 2, 3))


  }
}
