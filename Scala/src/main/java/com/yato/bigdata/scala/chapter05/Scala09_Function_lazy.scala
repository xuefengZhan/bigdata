package com.yato.bigdata.scala.chapter05

/**
 * yatolovefantasy
 * 2021-09-02-1:10
 */
object Scala09_Function_lazy {
  def main(args: Array[String]): Unit = {
    def fun9(): String = {
      println("function...")
      "zhangsan"
    }

//    val a = fun9()
//    println("----------")
//    println(a)
//    //打印结果：  顺序执行
//    //          function...
//    //          --------
//    //          zhangsan
      //我们假设fun9()的返回值是一个占用很大内存的对象
      //那么在a一直引用此对象的时候
      //假设println("----------") 这一句执行10min
      //这意味着一个很大的对象一直占用内存,一直没使用
      //所以这段时间一直挤占内存

      //那么能不能等到用到这个对象的时候 才获取此对象

      lazy val a = fun9()
      println("----------")
      println(a)
    //打印结果：
    //          --------
    //          function...
    //          zhangsan

    //todo 这表示只有用到这个函数对象的时候，才会执行这个函数  这就是惰性函数
  }
}
