package scala.chapter06

/**
 * yatolovefantasy
 * 2021-09-05-22:30
 */
object Scala11_Object_Abstract {
  def main(args: Array[String]): Unit = {

  }
  abstract class Parent{
    var name : String

    def run():Unit
  }
  class Child extends  Parent{

    var name: String = _

    def  run() : Unit = {
    }
  }

}
