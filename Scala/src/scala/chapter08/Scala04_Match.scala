package scala.chapter08

/**
 * yatolovefantasy
 * 2021-09-11-11:09
 */
object Scala04_Match {
  def main(args: Array[String]): Unit = {

    val list = List(
      ("a", 1), ("b", 2), ("c", 3)
    )

    //需求：将tuple中value 扩大1倍
    //第一种方式
    list.toMap.mapValues(cnt=>cnt*2)

    //第二种方式
    list.map(t=>{
      (t._1,t._2*2)
    })

    //第三种方式 匹配函数的参数
//  错误写法： 经常这么写错
//    list.map(
//      (word,cnt)=>{
//        (word,cnt*2)
//      }
//    )
//   错误原因： (word,cnt) 是参数列表，并不被认为是一个tuple参数

    //用模式匹配，跨越多行 用小括号不合适，所以map后面用{}
    val list1 = list.map {
      case (k, v) => {
        (k, v * 2)
      }
    }
    println(list1)

    val list2 = List((1,"张三"),30)
    list2.map{
      case ((prev,item),cnt)=>{
        (prev,(item,cnt))
      }
    }

    //参数匹配中，如果某个参数不关心 用不到，可以用_
      list.filter{
        case(_,cnt)=>{
          cnt%2==1
        }
      }
  }
}
