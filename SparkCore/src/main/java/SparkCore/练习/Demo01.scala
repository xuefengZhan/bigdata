package SparkCore.练习

import org.apache.spark.{SparkConf, SparkContext}

object Demo01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(" ")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array(("spark",2),("hadoop",6),("hadoop",4),("spark",6)))


    rdd.aggregateByKey((0,0))(
      (tuple,value)=>{
        (tuple._1 + value,tuple._2 + 1)
      },
      (tuple1,tuple2) =>{
        (tuple1._1 + tuple2._1,tuple1._2 + tuple2._2)
      }
    )

  }
}
