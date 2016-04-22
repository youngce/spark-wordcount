package sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mark on 2016/4/20.
  */
object ColOperations extends App{

  val sc: SparkContext=new SparkContext(new SparkConf().setAppName("sql").setMaster("local")) // An existing SparkContext.
  val sqlCtx = new org.apache.spark.sql.SQLContext(sc)
//
  val df: DataFrame = sqlCtx.read.json("src/main/resources/sql/people.json")

  val df1=df.select(df("age")+1 as "age",df("name"))

  df1.filter(df1("age")>20&&df1("age")<35)
    .show()


}
