package scala.chapter07

/**
 * yatolovefantasy
 * 2021-09-06-21:07
 */
object Scala01_Collection_Array2 {

  def main(args: Array[String]): Unit = {

    val array: Array[Int] = new Array[Int](5)

    //todo 1.修改   两种写法等价    修改元素是在原数组上操作
    array.update(1,5)
    array(1) = 5
    println(array.mkString(","))  //0,5,0,0,0

    //todo 2.添加元素
    //todo 2.1 头部添加
    val  Array1 = 5 +:  array  // +: 是运算符 等价与 array调用方法  +： ， 传入参数5
    val newArray: Array[Int] = array.+:(5)
    // 前者是运算符，后者是方法
    //scala中如果一个运算符使用冒号结尾，运算符则从后往前，从右往左
    //对Array的操作，增加元素，之前的数组不会改变，会产生新的数组
    // +: 往数组头部添加元素

    println(array.eq(newArray))  //eq比较内存地址

    println(newArray.mkString(","))//5,0,5,0,0,0
    println(Array1.mkString(","))//5,0,5,0,0,0
    //todo 2.2 尾部添加
    val Array2 = array :+ 5
    val array3: Array[Int] = array.:+(5)
    println(Array2.mkString(",")) //0,5,0,0,0,5

  }

}
