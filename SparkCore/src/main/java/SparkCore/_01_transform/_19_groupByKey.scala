package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

//def groupByKey(): RDD[(K, Iterable[V])]
//def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]
//def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]
object _19_groupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)

    //todo 1.groupBykey 和 groupBy的区别
    val rdd : RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 1), ("b", 2), ("c", 3),("a", 2)))

    val rdd1 : RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val rdd2: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    //todo 2.groupBykey 也可以指定分区器
    val rdd3: RDD[(String, Iterable[Int])] = rdd.groupByKey(new HashPartitioner(2))

    rdd1.glom().foreach(x=>println(x.mkString(","))) //(a,CompactBuffer(1,2)),(b,CompactBuffer(2)),(c,CompactBuffer(3))
    rdd2.glom().foreach(x=>println(x.mkString(","))) //(a,CompactBuffer((a,1), (a,2))),(b,CompactBuffer((b,2))),(c,CompactBuffer((c,3)))
  }
}
