package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL01 {
  def main(args: Array[String]): Unit = {
    //todo 1.创建上下文环境配置对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    //todo 2.创建SparkSession  上下文环境对象
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //todo 3.
    //RDD 转换到DataFrame和DataSet需要隐式转换
    //sparkSession是上下文环境对象
    import sparkSession.implicits._

    //todo 4.读取json文件，创建DataFrame  {"username": "lisi","age": 18}
    val df: DataFrame = sparkSession.read.json("input/test.json")

    //df.show()
    //+---+--------+
    //|age|username|
    //+---+--------+
    //| 18|    lisi|
    //+---+--------+

    //SQL风格  必须创建临时表
    //df.createOrReplaceTempView("user")
    //sparkSession.sql("select avg(age) from user").show
    //+--------+
    //|avg(age)|
    //+--------+
    //|    18.0|
    //+--------+

    //DSL风格
    //df.select("username","age").show()
    //+--------+---+
    //|username|age|
    //+--------+---+
    //|    lisi| 18|
    //+--------+---+



    //todo RDD 转 DF 转DS
    val rdd1: RDD[(Int, String, Int)] = sparkSession.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",28),(3,"wangwu",20)))
    //DataFrame  rdd加上字段名
    val df1: DataFrame = rdd1.toDF("id","name","age")
    //df1.show()
    //+---+--------+---+
    //| id|    name|age|
    //+---+--------+---+
    //|  1|zhangsan| 30|
    //|  2|    lisi| 28|
    //|  3|  wangwu| 20|
    //+---+--------+---+

    //dataSet   df加上表名
    val ds1: Dataset[User] = df1.as[User]
    //ds1.show()


    //todo *****DataSet=>DataFrame=>RDD*****
    //DataFrame
    val df2: DataFrame = ds1.toDF()

    //RDD
    val rdd2: RDD[Row] = df2.rdd
    //返回的RDD类型为Row，里面提供的getXXX方法可以获取字段值，类似jdbc处理结果集，但是索引从0开始
    rdd2.foreach(a=>println(a.getString(1)))


    //todo *****RDD=>DataSet*****
    //tuple 映射为Bean，在toDS()
    rdd1.map{
      case (id,name,age)=>User(id,name,age)
    }.toDS()

    //todo *****DataSet=>=>RDD*****
    ds1.rdd

    //释放资源
    sparkSession.stop()

  }
}
case class User(id:Int,name:String,age:Int)