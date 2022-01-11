package com.yato.bigdata.scala.chapter05

/**
 * yatolovefantasy
 * 2021-09-02-0:24
 */
object Scala07_Function_Curry {
  def main(args: Array[String]): Unit = {
    //函数柯里化 - Curry  是个科学家

    val i = 100
    val j = 200
    def test(i:Int,j:Int):Unit={
      for(a <- 1 to i){
        println(a)
      }

      for(b <- 1 to j){
        println(b)
      }
    }

    // todo 问题： test方法必须 有i 和 j两个参数才能执行
    //    但是test方法中，用到i的代码逻辑 和用到j的代码逻辑是完全独立的，二者不相干
    //    也就是说有i了，就能执行上面的for循环了，不需要等到j也传递进来；同理j也是
    // 科学家Curry发现了方法参数必须全部准备完毕，才能调用函数
    // 但是如果参数之间没有关系，那么会导致性能下降（等待两个参数都准备好），同时增加了业务复杂度；
    // 所以可以将参数通过参数列表给分离开

    // todo 函数支持多个参数列表，每个参数列表表示一个含义，将复杂逻辑简单化
    def test1(i:Int)(j:Int):Unit={
      i + j
    }
    // todo 核心要点： 函数中，参数之间相互独立，二者涉及的逻辑互不干扰

    // todo 应用
    // 将传值和 逻辑分离
    def operator(x:Int,y:Int)(op:(Int,Int)=>Int) : Int = {
      op(x,y)
    }

    operator(10,20)(_+_)

  }

}
