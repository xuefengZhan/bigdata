package scala.chapter02

/**
 * yatolovefantasy
 * 2021-08-28-23:16
 */
object Scala01_Var {
  def main(args: Array[String]): Unit = {
    //todo 1.var 声明的变量是可以修改的
    var name : String = "zhangsan"
    name = "wangwu"

    //todo 2.val 声明的变量是不能修改的
    val username : String = "lisi"
    //username = "zhaoliu"

    //todo 3.可以根据变量值的类型推断出变量类型，那么声明中可以省略类型
    val age = 18
  }
}
