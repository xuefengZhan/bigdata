package scala.chapter04

/**
 * yatolovefantasy
 * 2021-08-29-17:27
 */
object Scala01_Follow {

  def main(args: Array[String]): Unit = {

    val i = 10
    //todo 1.只有个if 返回值类型是Unit和 返回值的最小公共类
    // Int 和 Unit 公共父类 是AnyVal
    val result =
      if( i == 20){
      20
    }
    print(result)

    //todo 2. if else 多个判断语句，返回值是最小公共类
    // Any
    val result1 =
      if(i==20){
        20
      }else{
        "abc"
      }
    print(result1)

    //todo 3.AnyVal
    val result2 =
      if(i==20){
        20
      }else if(i==30){
        30
      }
    print(result2)
  }

}
