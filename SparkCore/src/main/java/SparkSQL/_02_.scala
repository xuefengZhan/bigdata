//package SparkSQL
//
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
//
///**
// * yatolovefantasy
// * 2021-10-11-22:52
// */
//object SparkSQL01_Demo {
//  def main(args: Array[String]): Unit = {
//    //todo 1.创建上下文环境配置对象
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
//
//    //todo 2.创建SparkSession对象
//    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//
//    //todo 3.隐式转换，为了用dataFrame dataSet
//    //RDD=>DataFrame=>DataSet转换需要引入隐式转换规则，否则无法转换
//    //spark不是包名，是上下文环境对象名
//    import spark.implicits._
//
//    //读取json文件 创建DataFrame  {"username": "lisi","age": 18}
//    val df: DataFrame = spark.read.json("input/test.json")
//    //df.show()
//
//    //todo 4. DataFrame 的 SQL风格语法
//    df.createOrReplaceTempView("user")
//    //spark.sql("select avg(age) from user").show
//
//    //todo 5.DSL风格语法
//    //df.select("username","age").show()
//
//    //todo 6. *****RDD=>DataFrame=>DataSet*****
//    //RDD
//    val rdd1: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",28),(3,"wangwu",20)))
//
//    //DataFrame
//    val df1: DataFrame = rdd1.toDF("id","name","age")
//    //df1.show()
//
//    //DateSet
//    val ds1: Dataset[User] = df1.as[User]
//    //ds1.show()
//
//    //*****DataSet=>DataFrame=>RDD*****
//    //DataFrame
//    val df2: DataFrame = ds1.toDF()
//
//    //RDD  返回的RDD类型为Row，里面提供的getXXX方法可以获取字段值，类似jdbc处理结果集，但是索引从0开始
//    val rdd2: RDD[Row] = df2.rdd
//    //rdd2.foreach(a=>println(a.getString(1)))
//
//    //*****RDD=>DataSet*****
//    rdd1.map{
//      case (id,name,age)=>User(id,name,age)
//    }.toDS()
//
//    //*****DataSet=>=>RDD*****
//    ds1.rdd
//
//    //释放资源
//    spark.stop()
//  }
//}
//case class User(id:Int,name:String,age:Int)