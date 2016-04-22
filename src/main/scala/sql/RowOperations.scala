package sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mark on 2016/4/20.
  */
object RowOperations extends App{

  val sc: SparkContext=new SparkContext(new SparkConf().setAppName("sql").setMaster("local")) // An existing SparkContext.
  val sqlCtx = new org.apache.spark.sql.SQLContext(sc)
  //

  import sqlCtx.implicits._

  // Define the schema using a case class.
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface.
  case class Person(name: String, age: Int)

  // Create an RDD of Person objects and register it as a table.
  val people = sc.textFile("src/main/resources/sql/people.json")
    .map(_.split(","))
    .map(p => Person(p(0), p(1).trim.toInt)).toDF()
  people.registerTempTable("people")

  // SQL statements can be run by using the sql methods provided by sqlContext.
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
