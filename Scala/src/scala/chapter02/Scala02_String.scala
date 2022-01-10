package scala.chapter02

/**
 * yatolovefantasy
 * 2021-08-29-12:43
 */
object Scala02_String {

  def main(args: Array[String]): Unit = {

    //todo 1.传值字符串  printf
    val name : String = "zhangsan"
    printf("name=%s\n", name)


    //todo 2.插值字符串  将变量值插入到字符串
    // 要在字符串外面加一个s  否则就是一个普通的字符串
    println(s"name=${name}")
    println("name=${name}")
    //注意 插值字符串在json格式的字符串中不要使用，会出现错误

    //todo 3.多行字符串
    // 应用场景：Json 和 SQL 字符串
    // 三个双引号， |是顶格符 是为了将输出结果靠左对齐用的
    println(
      s"""
        |{"name":"${name}"}
        |""".stripMargin)

  }

}
