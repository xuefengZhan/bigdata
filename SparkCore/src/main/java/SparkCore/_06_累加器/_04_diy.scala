package SparkCore._06_累加器

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable





/**
 * yatolovefantasy
 * 2021-10-10-23:00
 *
 * 自定义累加器 实现wordCount
 */
object _04_diy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(" ")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("hello","Spark","hello"))

    //todo 1.创建累加器
    val wcAcc = new myAcc()
    //todo 2.注册
    sc.register(wcAcc,"wordCountAcc")

    //todo 3.使用
    rdd.foreach(
      word=>{
        wcAcc.add(word)
      }
    )
    //todo 4.获取
    println(wcAcc.value)
  }

}
//6个方法
//泛型：IN 累加器输入的数据  out 累加器输出的数据类型
class myAcc extends AccumulatorV2[String,mutable.Map[String,Long]] {
  //创建一个Map做返回值
  private var wcMap = mutable.Map[String,Long]()

    //累加
  override def add(word: String): Unit = {
    val cnt: Long = wcMap.getOrElse(word, 0l)
    wcMap.update(word,cnt+1)
  }

  override def value: mutable.Map[String, Long] = {
    wcMap
  }
  //Driver端合并多个累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    val map1 = this.wcMap
    val map2 = other.value

    map2.foreach{
      case (word,cnt) =>{
        val newCnt = map1.getOrElse(word,0l) + cnt
        map1.update(word,newCnt)
      }
    }

  }

  override def isZero: Boolean = {
    wcMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
    new  myAcc()
  }

  override def reset(): Unit = wcMap.clear()





}