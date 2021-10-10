package com.yato.bigdata.scala.chapter05

/**
 * yatolovefantasy
 * 2021-09-01-1:06
 */
object Scala05_Function_Hell3 {
  def main(args: Array[String]): Unit = {
    def test(x:Int,y:Int,f: (Int,Int) => Int) = {
        f(x,y)
    }

    test( 1, 5, (x:Int,y:Int)=>{x+y} )
    test( 1,5,  (x,y)=>{x+y} )
    test( 1,5,  (x,y)=>x+y )
    test( 1, 5, _ + _)
  }
}
