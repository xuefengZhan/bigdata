package SparkCore._02_action

import org.apache.spark.{SparkConf, SparkContext}

object _10_save {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
    val rdd = sparkContext.makeRDD(List(("a",1),("b",2),("c",3),("a",3),("c",3)))

    // 保存成Text文件
    rdd.saveAsTextFile("output")

    // 序列化成对象保存到文件
    rdd.saveAsObjectFile("output1")

    // 保存成Sequencefile文件
   //  rdd.map((_,1)).saveAsSequenceFile("output2")
  }
}
