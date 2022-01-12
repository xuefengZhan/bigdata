package scala.chapter05

import scala.bean.Task

import java.io.ObjectInputStream
import java.net.{ServerSocket, Socket}

/**
 * yatolovefantasy
 * 2021-09-02-1:55
 */
object Scala11_Function_Worker {
  def main(args: Array[String]): Unit = {
    //todo 1.由于是master 往 worker 发送数据和计算逻辑，所以worker是服务器端
    val server = new ServerSocket(9999)
    val master: Socket = server.accept()

    val objIn = new ObjectInputStream(master.getInputStream)
    val task : Task = objIn.readObject().asInstanceOf[Task]
    objIn.close()
    master.close()

    for(i <- task.data){
      println(task.logic(i))
    }
    println("计算完毕")
    server.close()
  }
}
