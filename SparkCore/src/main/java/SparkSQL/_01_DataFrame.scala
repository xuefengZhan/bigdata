package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameReader, SparkSession}

object _01_DataFrame {
  def main(args: Array[String]): Unit = {
    //todo 1.创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //todo 2.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //RDD=>DataFrame=>DataSet转换需要引入隐式转换规则，否则无法转换

    val read: DataFrameReader = spark.read

    println(read)
  }
}
