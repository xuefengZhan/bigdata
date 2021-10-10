package com.yato.bigdata.scala.bean

class Task extends Serializable {
  // 数据
  val data = 1 to 5
  // 计算逻辑
  val logic = (x:Int) => x * 2
}
