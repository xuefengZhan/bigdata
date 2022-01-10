package SparkCore.framework.service

import SparkCore.framework.dao.wordCountDao
import org.apache.spark.rdd.RDD


//service层调用dao
class wordCountService {
  private val wordCountDao = new wordCountDao();

  //业务逻辑封装进方法中
  def dataAnalysis(): Array[(String,Int)]= {
    //todo 2.业务逻辑处理

    //数据源应该放进dao中，因为和业务逻辑无关
    //val fileRDD: RDD[String] = sc.textFile("SparkCore/target/classes/wc.txt")
    val fileRDD  = wordCountDao.readFile("SparkCore/target/classes/wc.txt")
    val rdd: RDD[(String, Int)] = fileRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    rdd.collect()

  }
}
