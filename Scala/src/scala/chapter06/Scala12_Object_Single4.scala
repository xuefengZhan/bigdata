package scala.chapter06

/**
 * 单例对象
 */
object Scala12_Object_Single4 {
  def main(args: Array[String]): Unit = {
    //半生类中的方法必须通过创建对象调用
    val student = new Student()
    student.test()
    //使用单例对象调用伴生对象中定义的方法 模拟静态语法
    Student.study()

    println(Student)  //Student$@71c7db30
    println(student)  //Student@19bb089b
    println(Student())//Student@4563e9ab
  }

  //半生类
  class Student {
    def test(): Unit = {}
  }

  //伴生对象
  object Student {

    def apply(): Student = new Student()

    def study(): Unit = {}

  }

}
