package SparkCore.framework.controller

import SparkCore.framework.service.wordCountService

//controller调用service层
class wordCountController {
  private val wordCountService = new wordCountService();


  //控制层调度（dispatch）或者执行(execute)
  def execute():Unit = {
    //Service层的业务逻辑
    val array: Array[(String, Int)] = wordCountService.dataAnalysis()

    array.foreach(println)

  }
}
