package scala.chapter08

/**
 * yatolovefantasy
 * 2021-09-10-21:58
 */
object Scala02_MatchTuple {
  def main(args: Array[String]): Unit = {
    for (tuple <- Array((0, 1), (1, 0), (1, 1), (1, 0, 2))) {
      val result = tuple match {
        case (0, _) => "0 ..."       //  匹配第一个元素是0的元组
        case (y, 0) => "" + y + "0" // 匹配后一个元素是0的对偶元组
        case (a, b) => "" + a + " " + b
        case _ => "something else" //默认
      }
      println(result)
    }
    //todo 说明：
    // case (0, _) => "0 ..."
    // case (y, 0) => "" + y + "0"  为什么这个不能用下划线？
    // 第一个可以用，因为 => 后面没用到这个
    // 第二个不可以用下划线，因为 => 后面用到了这个，所以要起个变量名 装起来


    //0 ...
    //10
    //1 1
    //something else


  }
}
