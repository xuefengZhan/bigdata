package scala.chapter07

object Scala09_Collection_paraller {
  def main(args: Array[String]): Unit = {
    val result1 = (0 to 100).map{x => Thread.currentThread.getName}
    //par 并行的缩写，并行集合   用多线程来处理每一条数据  处理顺序无法保障
    val result2 = (0 to 100).par.map{x => Thread.currentThread.getName}

    println(result1)
    println(result2)
  }
}
