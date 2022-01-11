package com.yato.bigdata.scala.chapter07

/**
 * yatolovefantasy
 * 2021-09-06-21:07
 */
object Scala01_Collection_Array {

  def main(args: Array[String]): Unit = {
    //todo 集合 数组

    //todo 1.创建数组  必须指定长度
    val array: Array[String] = new Array[String](5)
    println(array) //[Ljava.lang.String;@71c7db30  就是一个java数组
    //todo 2. 访问数组元素
    // 必须采用小括号
    array(0) = "张三"
    array(1) = "李四"

    //todo 3.遍历数组
    for(s <- array){
      println(s)
    }

    //todo 4.将集合生成字符串，参数为元素分隔符
    println(array.mkString(","))

    //todo 5. forach对元素中每个元素进行操作
    // 参数：f:String => U  是一个输出不确定的函数
    array.foreach(println(_))
    array.foreach(println)
  }

}
