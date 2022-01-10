package scala.chapter04

import scala.util.control.Breaks.{break, breakable}

/**
 * yatolovefantasy
 * 2021-08-29-19:53
 */
object Scala04_Follow_break {

  def main(args: Array[String]): Unit = {

    breakable{
      for ( i <- Range(1,5) ) { // 不包含5
        if(i==2){
          break
        }
        println(i)
      }
    }
    println("继续执行")
  }

}
