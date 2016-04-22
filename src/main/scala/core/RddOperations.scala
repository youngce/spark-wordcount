package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mark on 4/22/16.
  */
object RddOperations extends App{

  val conf=new SparkConf().setAppName("WordCount").setMaster("local")
  val sc=new SparkContext(conf)
  val txt: RDD[String] =sc.textFile("src/main/resources/wordcount.txt")

  val ints=sc.parallelize(1 to 100000)


  // Key-Value RDD
  val kvRdd =ints.map(v=>if (v%2==0) "even"->v else "odd"->v)
  //  kvRdd.map(kv=>kv._1->(kv._2+1)).foreach(println)
    //kvRdd.groupByKey().foreach(println)
  kvRdd.reduceByKey(_+_).foreach(println)

}
