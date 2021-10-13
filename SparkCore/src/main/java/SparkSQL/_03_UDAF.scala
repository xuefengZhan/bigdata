package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

object _03_UDAF {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/user.json")
    df.createOrReplaceTempView("user")
    //todo 注册自定义函数
    //sql不关注类型，所以将强类型操作转换为弱类型
    spark.udf.register("ageAvg",functions.udaf(new MyAvgUDAF()))

    spark.sql("select ageAvg(age) from user").show
    //+--------------+
    //|myavgudaf(age)|
    //+--------------+
    //|            20|
    //+--------------+

    spark.close()
  }
}

//todo 1.自定义聚合函数：计算年龄平均值

//1.继承org.apache.spark.sql.expressions.Aggregator
//2.泛型  IN:输入数据类型   BUF:buffer中的数据类型  OUT:输出的数据类型
case class Buff(var total:Long,var count:Long)
//用样例类作为缓冲区的数据类型，total是总的薪资，count是个数
//用var修饰属性，是因为银行里类默认是val不能修改

class MyAvgUDAF extends Aggregator[Long,Buff,Long] {
  //todo 3.缓冲区初始化
  override def zero: Buff = Buff(0L,0L)
  //todo 4.根据输入的数据更新缓冲区中的数据
  override def reduce(buff: Buff, in: Long): Buff = {
    buff.total = buff.total + in
    buff.count = buff.count + 1
    buff
  }
  //todo 5.合并缓冲区
  override def merge(buff1: Buff, buff2: Buff): Buff = {
    buff1.total = buff1.total + buff2.total
    buff1.count = buff1.count + buff2.count
    buff1
  }
  //todo 6.计算结果
  override def finish(buff: Buff): Long = {
    buff.total/buff.count
  }
  //todo 7.分布式计算 需要将数据进行网络中传输，所以涉及缓冲区序列化和编码问题
  //缓冲区的编码操作  自定义类就用这个Encoders.product
  override def bufferEncoder: Encoder[Buff] = Encoders.product
  //输出的编码操作
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}