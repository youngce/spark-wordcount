package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mark on 12/1/15.
  */
object WordCount extends App{
  //val Array(in, out)=args
  val conf=new SparkConf().setAppName("WordCount").setMaster("local")
  val sc=new SparkContext(conf)
  val txt: RDD[String] =sc.textFile("src/main/resources/wordcount.txt")

//  val kvRdd =sc.parallelize(1 to 100).map(v=>if (v%2==0) "even"->v else "odd"->v)
////  kvRdd.map(kv=>kv._1->(kv._2+1)).foreach(println)
//  //kvRdd.groupByKey().foreach(println)
//  kvRdd.reduceByKey(_+_).foreach(println)

  //nums.map(_+1).foreach(println)
//
  val result: RDD[(String, Int)] =sc.textFile("src/main/resources/wordcount.txt")
                .flatMap(_.split(" "))
                .map(_->1).reduceByKey((acc,x)=>acc+x)
//  val aa: Array[(String, Int)] =result.collect()//.foreach(println)
  result.foreach(println)
}
