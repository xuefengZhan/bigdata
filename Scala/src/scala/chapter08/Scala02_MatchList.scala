package scala.chapter08

/**
 * yatolovefantasy
 * 2021-09-10-21:58
 */
object Scala02_MatchList {
  def main(args: Array[String]): Unit = {
    for (list <- Array(List(0), List(1, 0), List(0, 0, 0), List(1, 0, 0), List(88))) {
      val result = list match {
        case List(0) => "0" //匹配List(0)
        case List(x, y) => x + "," + y //匹配有两个元素的List
        case List(0, _*) => "0 ..."
        case _ => "something else"
      }

      println(result)
    }


    val list: List[Int] = List(1, 2, 5, 6, 7)

    list match {
      case first :: second :: rest => println(first + "-" + second + "-" + rest)
      case _ => println("something else")
    }
    //1-2-List(5, 6, 7)

    val list2 : List[Int] = List(1, 2)
    list2 match{
      case first :: second :: rest => println(first + "-" + second + "-" + rest)
      case _ => println("something else")
    }
    //1-2-List()

    val list3 : List[Int] = List(1)
    list2 match{
      case first :: second :: rest => println(first + "-" + second + "-" + rest)
      case _ => println("something else")
    }
    //something else
  }
}
