package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object _20_aggregateByKey {
//todo def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U): RDD[(K, U)]
  // 注意： zeroValue的类型为 U     元素类型为：(K,V)
  // 经过计算后，输出的类型为 (K,U)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)


    val src =
      sparkContext.makeRDD(List(("a",1),("b",1),("a",2),("c",2),("b",2),("c",3)))
    val rdd1 =
      src.aggregateByKey("x")(_+_,_+_)

    rdd1.glom().foreach(x=>println(x.mkString(",")))

    //两个分区的结果：
    //(a,x12),(c,x33)
    //(b,x2x2)

    //todo zeroValue只参与分区内计算
    //计算过程：
    //分区1：("a",1),("b",1),("a",2)   分区2：("c",2),("b",2),("c",3)
    //分区内计算：
    //分区1：(a,x1) (b,x1)  => (b,x1) (a,x12)
    //分区2：(c,x2) (b,x2)  => (b,x2) (c,x23)
    //分区间计算：
    //(b,x1x2) (a,x12) (c,x33)
    //最终输出是两个分区，因此按照两个分区输出
    //(a,x12),(c,x23)
    //(b,x1x2)

    sparkContext.stop()
  }
}
