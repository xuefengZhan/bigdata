package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

//def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
//todo 当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey
//注意：aggregateByKey 的 zeroValue可以是任何类型，最终算子的输出的v 跟随zeroValue的类型
//但是foldByKey的 zeroValue的类型必须和(K,V)的V保持一致
object _21_foldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)


    val src =  sparkContext.makeRDD(List(("a",1),("b",2),("a",2),("c",3),("b",2),("c",3)))
    val rdd1 =  src.foldByKey(0)(_+_)

    rdd1.glom().foreach(x=>println(x.mkString(",")))
  }
}
