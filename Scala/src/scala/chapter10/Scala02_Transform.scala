package scala.chapter10

/**
 * yatolovefantasy
 * 2021-09-11-13:47
 */
object Scala02_Transform {
  def main(args: Array[String]): Unit = {

    //隐式参数
    def reg(implicit password : String = "000000") : Unit = {
      println("密码为："+password)
    }

    //隐式变量   注意和隐式参数类型一致
    implicit val password : String = "123123"

    //如果应用隐式参数，小括号可以省略
    reg  //密码为：123123
    //没有省略，不传值，隐式参数不起作用
    reg() //密码为：000000
    //传递参数，那么传递的就是最终参数
    reg("456456") //密码为：456456


  }
}
