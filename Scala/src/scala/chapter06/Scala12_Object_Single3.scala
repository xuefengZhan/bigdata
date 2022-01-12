package scala.chapter06

/**
 * 单例对象
 */
object Scala12_Object_Single3 {
  def main(args: Array[String]): Unit = {

    println(User)  //单例对象
    println(new User())//通过构造器创建的对象
    println(User()) //通过伴生对象的apply方法创建的对象
  }

  //半生类
  class User{}

  //伴生对象
  object User{

    def apply(): User = new User()

  }

}
