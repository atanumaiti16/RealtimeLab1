/**
  * Created by atanu on 9/6/16.
  */
import org.apache.spark.SparkContext

import org.apache.spark.SparkConf

object LAB2 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("lab2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("/home/atanu/PycharmProjects/twitter-analysis/Output3.txt")

    val SC=textFile.flatMap(line=>(line.split("\\. "))).map(word=>(word)).cache()

   // val df = textFile.

    val linesWithBrazilian = textFile.filter(line => line.contains("Brazilian"))

    println(textFile.filter(line => line.contains("Brazilian")).count())





    linesWithBrazilian.saveAsTextFile("/home/atanu/WORK/RTlab2")


  }
}