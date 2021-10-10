package com.yato.bigdata.scala.chapter07

/**
 * yatolovefantasy
 * 2021-09-06-21:07
 */
object Scala01_Collection_Array4 {

  def main(args: Array[String]): Unit = {
    //todo 多维数组  每个元素是一个数组

    var myMatrix = Array.ofDim[Int](3,3) //3*3数组
    myMatrix.foreach(list=> println(list.mkString(",")) )
    //0,0,0
    //0,0,0
    //0,0,0

    //todo 合并数组   相当于链接
    val arr1: Array[Int] = Array(1, 2, 3, 4)
    val arr2: Array[Int] = Array(5, 6, 7, 8)
    val arr6: Array[Int] = Array.concat(arr1, arr2)
    println(arr6.mkString(",")) //1,2,3,4,5,6,7,8


    //todo 创建指定范围的数组    [start,end)
    val arr7: Array[Int] = Array.range(0,2)
    println(arr7.mkString(","))  //0,1


    //todo 创建并填充指定数量的数组
    val arr8:Array[Int] = Array.fill[Int](5)(-1)
    arr8.foreach(println)

  }
}
