package SparkCore.framework.dao

import SparkCore.framework.application.wordCountApplication.sc
import org.apache.spark.rdd.RDD

class wordCountDao {

  def readFile(path:String) = {
    val fileRDD: RDD[String] = sc.textFile("SparkCore/target/classes/wc.txt")
    fileRDD
  }
}
