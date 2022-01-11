package com.yato.bigdata.scala.chapter07

/**
 * yatolovefantasy
 * 2021-09-09-20:25
 */
object Scala15_练习 {
  def main(args: Array[String]): Unit = {
    val list = List(
      ("zhangsan", "河北", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "鞋"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"),
      ("wangwu", "河南", "鞋"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "鞋"),
      ("zhangsan", "河北", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "帽子"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"),
      ("wangwu", "河南", "帽子"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "帽子"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "电脑"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"),
      ("wangwu", "河南", "电脑"),
      ("zhangsan", "河南", "电脑"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "帽子")
    )

    //需求：不同省份的商品点击排行

    //1.(省份，商品)
    val tuples: List[(String, String)] = list.map(x => (x._2, x._3))
    //2.Map(省份->List((省份，商品)))
    val keyList: Map[String, List[(String, String)]] = tuples.groupBy(t => t._1)
    //3.Map(省份->List(商品，Count))
    val provToWordCount: Map[String, Map[String, Int]] = keyList.map(kv => {
      val prov = kv._1 //省份
      val list = kv._2 //商品列表
      val stringToStrings: Map[String, List[String]] = list.map(_._2).groupBy(x => x)
      val wordCount: Map[String, Int] = stringToStrings.map(m => (m._1, m._2.size))
      prov -> wordCount
    })

    val result: Map[String, List[(String, Int)]] = provToWordCount.map(x => (x._1, x._2.toList.sortBy(_._2)(Ordering.Int.reverse)))
    println(result)//Map(河南 -> List((鞋,6), (衣服,3), (电脑,2), (帽子,1)), 河北 -> List((衣服,6), (鞋,4), (帽子,3), (电脑,1)))
  }

}
