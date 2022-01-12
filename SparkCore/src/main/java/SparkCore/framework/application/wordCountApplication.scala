package SparkCore.framework.application

import SparkCore.framework.controller.wordCountController
import org.apache.spark.{SparkConf, SparkContext}

object wordCountApplication extends App{
  //todo 1.准备环境
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
  val sc = new SparkContext(sparkConf)

  //todo 2.调度
  //将业务过程放到controller层
  val controller = new wordCountController()
  controller.execute()

  sc.stop()
}
