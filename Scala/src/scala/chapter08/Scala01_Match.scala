package scala.chapter08

/**
 * yatolovefantasy
 * 2021-09-10-1:06
 */
object Scala01_Match {

  def main(args: Array[String]): Unit = {
    val age = 30
    //模式匹配使用match和case关键字
    //按照case顺序进行匹配
    //匹配成功，执行箭头右边的代码，执行完毕后自动跳出，规避了穿透
    //匹配不成功，执行下划线后面的代码
    //如果不提供下划线分支，匹配不成功的情况下就会发生错误
    age match{
      case 10 => println("10")
      case 20 => println("20")
      case 30 => println("30")
      case _ => println("AnyVal")
    }
  }

}
