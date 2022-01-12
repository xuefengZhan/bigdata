package scala.chapter07

object Scala11_Collection_fold {
  def main(args: Array[String]): Unit = {
    val map1 = Map("a" -> 1, "b" -> 2)
    val map2 = Map("a" -> 3, "b" -> 4)

    //todo fold 折叠  聚合多个集合
    //fold 函数柯里化，两个参数列表
    //第一个参数列表是 集合外的数据，是初始值
    //第二个参数列表表示数据两两计算的规则
    val list = List(1,2,3,4,5)
    val i: Int = list.fold(0)(_ - _) // 0,1,2,3,4,5
    println(i)//-15

    //fold(z:A)((A,A)=>A)
    //foldLeft(z:B)((B,A)=>B)
    //foldRight   reversed.foldLeft(z)((x,y)=> op(y,x))
    val i1: Int = list.foldRight(0)(_ - _)  //  1,2,3,4,5,0 然后就是reduceRight过程了
    println(i1)//3


    //todo Scan 保留每一步操作的结果到List中
    val ints: List[Int] = list.scanRight(5)(_ - _) //1,2,3,4,5,5
    println(ints) //List(-2, 3, -1, 4, 0, 5)

  }
}
