package com.yato.bigdata.scala.chapter07

object Scala10_Collection_reduce {
  def main(args: Array[String]): Unit = {
    //TODO 集合 - 计算方法 - 聚合功能函数
    //Scala集合提供了一些用于聚合数据功能的函数
    //但是聚合逻辑不确定
    // 1->N flatMap
    // N->1 reduce (聚合)

    //todo 1.reduce 将集合中的元素两两聚合，聚合规则自己制定
    val list = List(1, 2, 3, 4, 5)
    //参数是(A1,A1)=>A1   两个操作数和返回值类型一致
    //Scala中数据操作一般都是两两操作
    val i: Int = list.reduce(_-_)//-1 -4 -9 -13
    println(i)//10

    //todo 2.reduceLeft
    //参数： （A，B）=>A   两个操作数类型可以不一致，返回值和第一个操作数一致
    val i1: Int = list.reduceLeft(_ - _) //-1 -4 -9 -13
    println(i1)

    //todo 3.reduceRight
    // 先反转，再reduceLeft  (5,4,3,2,1)
    val i2: Int = list.reduceRight(_ - _) //-1 4 -2 3
    println(i2)

    //todo 4.理解reduceLeft和reduceRight
    //reduceLeft从左加括号  ((((1, 2), 3), 4), 5)  再将逗号变为运算符  结果即：  -1 -4 -5 -10
    //reduceRight从右加括号 (1, (2, (3,( 4, 5))))                           -1 4 -2 3
  }
}
