package SparkCore._03_serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _03_序列化{

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    //一个Search对象
    val search = new Search("hello")

    //todo 函数传递，打印：ERROR Task not serializable
    search.getMatch1(rdd).collect().foreach(println)


    //todo  属性传递，打印：ERROR Task not serializable
    search.getMatch2(rdd).collect().foreach(println)


    search.getMatch3(rdd).collect().foreach(println)


    sc.stop()
  }
}

//TODO 查询对象
class Search(query:String) extends Serializable {
  //todo  1.用外置函数查询rdd中的元素有没有包含query
  def isMatch(s: String): Boolean = {
    s.contains(query)  // todo  这个query = this.query 是Search类的属性
  }
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //todo getMatch1方法中调用了rdd.filter(isMatch)
  // filter算子中的isMatch方法只用到了query 字符串类型，为什么也不能通过？
  // 原因：类的构造参数是类的属性，query就是Search类的属性
  // 反编译就能看到了  在java中是  private final String query
  // 构造参数需要进行闭包尖刺，其实就等同于类进行闭包检测

  //todo 2.用匿名函数方式查询
  def getMatch2(rdd: RDD[String]): RDD[String] = {

    rdd.filter(x => x.contains(query))
    //val q = query
    //rdd.filter(x => x.contains(q))
  }

  def getMatch3(rdd: RDD[String]): RDD[String] = {
    //todo 3.getMatch2的改进
    // 调用getMatch3的时候Search不用序列化也能通过
    val q = query
    rdd.filter(x => x.contains(q))

    // val q = query 在driver端执行
    //filter算子中的逻辑执行在Executor端
    //filter算子闭包引用的是q，q是方法的局部变量，和类没有关系
    //形成闭包，q是字符串类型，可序列化
  }
}