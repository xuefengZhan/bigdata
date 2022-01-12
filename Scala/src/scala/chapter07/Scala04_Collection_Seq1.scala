package scala.chapter07

import scala.collection.mutable.ListBuffer

/**
 * yatolovefantasy
 * 2021-09-08-1:22
 */
object Scala04_Collection_Seq1 {
  def main(args: Array[String]): Unit = {
   val list : ListBuffer[Int] = ListBuffer(1, 2, 3, 4)
   list.append(5)
   list.update(1,10)
   list.remove(2)

   val str: String = list.mkString(",")


  }
}
