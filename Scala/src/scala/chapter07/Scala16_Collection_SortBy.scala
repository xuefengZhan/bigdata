package scala.chapter07

/**
 * yatolovefantasy
 * 2021-09-09-20:25
 */
object Scala16_Collection_SortBy {
  def main(args: Array[String]): Unit = {
   val user1 = new User()
   user1.id = 1
    user1.age = 10

    val user2 = new User()
    user2.id = 2
    user2.age = 20

    val user3 = new User()
    user3.id = 2
    user3.age = 30

    val list = List(user1,user2,user3)

    //list.sortBy(_.id)

    //todo sortWith List的方法，自定义排序
    //两个两个排，排序方式符合预期返回true
    val users: List[User] = list.sortWith(
      (left, right) => {
        if (left.id < right.id) {
          true
        } else if (left.id > right.id) {
          false
        } else {
          left.age > right.age
        }
      }
    )

    println(users)
    //List(User[1][10], User[2][30], User[2][20])
  }


  class User{
    var id : Int = _
    var age : Int = _

    override def toString: String = {
      s"User[${id}][${age}]"
    }
    }
}
