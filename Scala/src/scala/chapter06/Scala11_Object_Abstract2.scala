package scala.chapter06

/**
 * yatolovefantasy
 * 2021-09-05-22:30
 */
object Scala11_Object_Abstract2 {
  def main(args: Array[String]): Unit = {
      new Child().run()
  }
  class Parent{
    val name : String ="张三"
    def run():Unit={
      println(name)//李四
    }
  }
  class Child extends  Parent{
    override val name : String = "李四"
    override def  run() : Unit = {
      super.run()
    }
  }
}
