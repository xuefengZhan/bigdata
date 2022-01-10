package scala.chapter05

/**
 * yatolovefantasy
 * 2021-09-01-1:43
 */
object Scala05_Function_Hell5 {
  def main(args: Array[String]) :Unit = {
      def outer(x:Int)  = {
          def middle(y:Int) ={
            def inner(f:(Int,Int)=>Int) ={
                f(x,y)
            }
            inner _
          }
        middle _
      }

    //使用
    println(outer(10)(10)(_ + _))
  }
}
