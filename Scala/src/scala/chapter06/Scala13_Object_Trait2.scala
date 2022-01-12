package scala.chapter06

object Scala13_Object_Trait2 {
  def main(args: Array[String]): Unit = {
    val user = new User() with UserExt
    user.updateUser()
  }


  trait UserExt {
   def updateUser() : Unit = {
     println("update user...")
   }
  }


  class User{
    def insertUser() : Unit = {
      println("insert user...")
    }
  }

}
