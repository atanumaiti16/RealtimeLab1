/**
  * Created by atanu on 8/30/16.
  */
import org.apache.spark.SparkContext

import org.apache.spark.SparkConf

object sortingSentence {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("sortingSentence")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/home/atanu/WORK/wordcount")


    val SC=textFile.flatMap(line=>(line.split("\\. "))).map(word=>(word)).cache()
    val sorted = SC.sortBy[String]({a => a})
    sorted.saveAsTextFile("/home/atanu/WORK/result13")


  }
}