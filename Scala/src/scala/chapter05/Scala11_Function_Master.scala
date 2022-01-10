package scala.chapter05
import scala.bean.Task


import java.io.ObjectOutputStream
import java.net.Socket

/**
 * yatolovefantasy
 * 2021-09-02-1:55
 */
object Scala11_Function_Master {
  def main(args: Array[String]): Unit = {
     val worker = new Socket("localhost",9999)
    val objOut = new ObjectOutputStream(worker.getOutputStream)


    val task = new Task()
    objOut.writeObject(task)

    objOut.flush()
    objOut.close()
    worker.close()



  }
}
