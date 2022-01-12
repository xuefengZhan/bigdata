package scala.chapter05

/**
 * yatolovefantasy
 * 2021-08-31-23:14
 */
object Scala05_Function_Hell1 {
  def main(args: Array[String]): Unit = {

      def test(i:Int) : Int = {
        i*2
      }

    //todo 将函数对象传值给变量要解决两个问题：
    // 1.如何将函数对象传递给变量
    // 2. 函数对象的类型是如何定义的
     val f : (Int)=>Int = test
     println(f(1))

    // 函数类型中，当函数参数只有一个的时候是可以省略小括号的
    // 但是函数没有参数的时候，小括号一般不要省略
    val f1 : Int =>Int = test


    def test1(name :String,password:String) : String = {
      name + "," + password
    }

    val f2 : (String,String)=>String = test1
    println(f2("张三", "123"))
  }
}
