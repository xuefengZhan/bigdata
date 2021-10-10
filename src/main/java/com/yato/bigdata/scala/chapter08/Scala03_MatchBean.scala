package com.yato.bigdata.scala.chapter08

/**
 * yatolovefantasy
 * 2021-09-10-22:06
 */
object Scala03_MatchBean {


  class User(val name: String, val age: Int)
  //类的构造方法中参数使用val或者var声明 那么这个参数直接作为类的属性使用
  //val属性不能改  var的可以改
  // val u = new User("zhangsan", 20)

  object User{

    //伴生对象的apply方法 是将数据封装成一个对象
    def apply(name: String, age: Int): User = new User(name, age)

    //而unapply方法正好是反过来的，将对象拆分成数据
    def unapply(user: User): Option[(String, Int)] = {
      if (user == null)
        None
      else
        Some(user.name, user.age)
    }
  }

  val user: User = User("zhangsan", 11)  //通过伴生对象的apply创建对象
  val result = user match {
    case User("zhangsan", 11) => "yes"
    case _ => "no"
  }
}
