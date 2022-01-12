package scala.chapter07

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * yatolovefantasy
 * 2021-09-08-1:22
 */
object Scala05_Collection_Set {
  def main(args: Array[String]): Unit = {
      val set = Set(1,2,3,4,5,3,2,1)
       println(set) //Set(5, 1, 2, 3, 4)

      val newSet = set + 7

      val newSet1 = newSet - 5


    //todo 可变set集合 可以直接用包名来指定
    val sets : mutable.Set[Int] = mutable.Set(1, 2, 3, 4, 5, 3, 2, 1)

    sets.add(7)  //增加元素会打乱顺序
    sets.update(8,true)//true表示认为集合中包含元素8，如果没有就给你添加   false认为不包含，有就给你删除
    sets.remove(5)

    sets + 7  // + 会返回新的集合
    sets += 7 // += 不会返回新的集合，只做添加

  }
}
