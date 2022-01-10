package scala.chapter05

/**
 * yatolovefantasy
 * 2021-09-01-1:56
 *
 * day3. 6闭包
 */
object Scala05_Function_Hell6 {
  def main(args: Array[String]): Unit = {
    // todo 1.Scala是基于Java的，因此函数在字节码文件中一定是方法
    // todo 2.参数x编译后应该是方法的局部变量
    //  局部变量的作用域是当前方法内部
    // todo 3.inner函数 在字节码文件中也是方法
    //  inner方法的执行 在outer方法执行之后 执行  => 因为inner方法就是outer方法的执行结果
    // todo 4.问题
    //  outer方法执行完了，栈帧弹出  局部变量x也会被回收
    //  此时inner入栈, 那么inner为什么能用x
    def outer(x:Int) = {
      def inner(y:Int) = {
        x+y
      }
      inner _
    }

    println(outer(20)(20))
  }
}
