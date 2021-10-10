package SparkCore._02_action

import org.apache.spark.{SparkConf, SparkContext}

object _09_countByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
    val dataRDD1 = sparkContext.makeRDD(List(("a",1),("b",2),("c",3),("a",3),("c",3)))

    val nums: collection.Map[String, Long] = dataRDD1.countByKey()


    dataRDD1.countByValue().foreach(println)
    //((b,2),1)
    //((a,3),1)
    //((c,3),2)  相同的元素才算一个
    //((a,1),1)

    nums.foreach(println)
    //(a,2)
    //(b,1)
    //(c,2)

  }
}
