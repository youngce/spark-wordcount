package sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mark on 2016/4/19.
  */
object DSLAndSQL extends App{
  val sc: SparkContext=new SparkContext(new SparkConf().setAppName("sql").setMaster("local")) // An existing SparkContext.
  val sqlCtx = new org.apache.spark.sql.SQLContext(sc)

  val df: DataFrame = sqlCtx.read.json("src/main/resources/sql/people.json")
  df.show()


  // DSL
  println("=====DSL=======")
  df.filter(df("age")>20).select("name").show()

  // SQL statement
  println("=====SQL statement=======")
  df.registerTempTable("people")
  sqlCtx.sql("SELECT name FROM people where age >20").show()
}
