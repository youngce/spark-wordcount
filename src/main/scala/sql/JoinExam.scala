package sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by mark on 2016/4/20.
  */
object JoinExam extends App{
  val sc: SparkContext=new SparkContext(new SparkConf().setAppName("sql").setMaster("local")) // An existing SparkContext.
  val sqlCtx = new org.apache.spark.sql.SQLContext(sc)
  //
  val df: DataFrame = sqlCtx.read.json("src/main/resources/sql/people.json")
  import sqlCtx.implicits._
  case class Person(id:Int,name: String, age: Int)

  case class Address(personId:Int,country:String)
  // Create an RDD of Person objects and register it as a table.
  val people = sc.parallelize(Seq(Person(1,"mark",30),Person(2,"jason",22))).toDF()
  val addresses = sc.parallelize(Seq(Address(1,"Taipei"),Address(1,"I-Lan"),Address(2,"Taichung"))).toDF()

  people.join(addresses,people("id")===addresses("personId")).show()


  people.registerTempTable("people")
  addresses.registerTempTable("addresses")
  sqlCtx.sql(
    """
      SELECT * FROM people p
      JOIN addresses add
      ON p.id=add.personId
    """.stripMargin).show()


}
