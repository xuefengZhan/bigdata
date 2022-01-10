package scala.chapter05

/**
 * yatolovefantasy
 * 2021-08-31-23:14
 */
object Scala05_Function_Hell2 {
  def main(args: Array[String]): Unit = {

    //todo 将参数作为参数进行传递
    def test(f: (String) => String ) : String = {
         f("张三")
    }

    //定义函数
    def f(s : String) : String = {
      s * 2
    }

    //函数对象
    val v = f _
    // 函数对象传递进去
    println(test(v))
    println(test(f _))
    println(test(f))


    // todo 2.匿名函数 作为参数
    val a = (s: String) => {s * 2}
    println(test(a))

    // todo 2.1 匿名函数直接做参数
    test( (s: String) => {s * 2} )

    // todo 2.2 根据著参数可以推断出参数（匿名函数对象）的类型，因此可以省略匿名函数的参数类型声明
    test( (s)=>{s * 2} )
    // todo 2.3 如果匿名函数参数列表只有一个，省略小括号
    test( s => {s * 2})
    // todo 2.4 如果匿名函数方法体只有一行逻辑，可以省略大括号
    test( s => s * 2 )
    // todo 2.5 如果匿名函数的参数只使用了一次，可以省略参数,用下划线代替
    test( _ * 2)
  }
  }

