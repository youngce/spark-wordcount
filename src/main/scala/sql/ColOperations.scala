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
//
//  val df1=df.select(df("age")+1 as "age",df("name"))
//
//  df1.filter(df1("age")>20&&df1("age")<35)
//    .show()

  df.registerTempTable("people")
  val teenagers = sqlCtx.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")

  // The results of SQL queries are DataFrames and support all the normal RDD operations.
  // The columns of a row in the result can be accessed by field index:
  teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

  // or by field name:
  teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)

  // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
  teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
  // Map("name" -> "Justin", "age" -> 19)
}
