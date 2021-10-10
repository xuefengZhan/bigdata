package com.yato.bigdata.scala.chapter02

object Scala03_DataType {
  def main(args: Array[String]): Unit = {

    // a为NULL类型
   val a = null;
    // 多态
    val b : String = null;
  }

  def test() : Nothing = {
    throw new Exception
  }

}
