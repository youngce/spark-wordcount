import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by mark on 12/1/15.
  */
object WordCount extends App{
  val Array(in, out)=args
//  val in=args(0)
//  val out=args(1)
  val conf=new SparkConf().setAppName("WordCount")
  val sc=new SparkContext(conf)
  val result=sc.textFile(in).flatMap(_.split(" ")).map(_->1).reduceByKey(_+_)
  result.saveAsTextFile(out)
}
