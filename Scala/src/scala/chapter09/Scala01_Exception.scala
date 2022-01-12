package scala.chapter09

/**
 * yatolovefantasy
 * 2021-09-11-11:49
 */
object Scala01_Exception {

  def main(args: Array[String]): Unit = {
    //编译期异常 =》 提示性异常
    //运行时异常 => BUG

    //scala中异常不分类
    //scala任何异常都不需要显示处理
    //异常处理和模式匹配非常类似
    try {
      val x = 0
      val y = 10 / x
    } catch {
      //捕获异常的时候，范围小的放在前面，大的放在后面
      //如果没有异常匹配到，会直接将异常抛出
      case e: Exception => e.printStackTrace()
    }finally{

    }

  }

}
