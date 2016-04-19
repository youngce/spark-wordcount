package core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mark on 12/1/15.
  */
object WordCount extends App{
  //val Array(in, out)=args
  val conf=new SparkConf().setAppName("WordCount").setMaster("local[*]")
  val sc=new SparkContext(conf)
  val result=sc.textFile("src/main/resources/wordcount.txt").flatMap(_.split(" ")).map(_->1).reduceByKey(_+_)
  result.foreach(println)
}
