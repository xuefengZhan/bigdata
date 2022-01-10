package scala.chapter05

/**
 * yatolovefantasy
 * 2021-09-01-1:43
 */
object Scala05_Function_Hell4 {
  def main(args: Array[String]): Unit = {
    //todo 1.第一种方式：不主动声明返回函数类型，但是返回的函数对象要加_
    // 推荐使用这种方式
    def test() = {
      def inner() : Unit = {
        println("xxxxxxx")
      }
      inner _
    }
    //todo 2.第二种方式：主动声明返回函数类型，返回的函数对象不需要加_
    def test1() : ()=>Unit = {
      def inner() : Unit = {
        println("xxxxxxx")
      }
      inner
    }

    //todo 3.使用
    val f = test()
    f()

    //todo 3.2 使用
    test()()

  }

}
