/**
  * Created by mathiasedouin on 13/01/17.
  */

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

object DataAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("DAdemo").setMaster("local[2]")
    val scontext = new SparkContext(sparkconf)
    val rootlogger = Logger.getRootLogger().setLevel(Level.ERROR)
    val sqlcontext = new SQLContext(scontext)

    val distFile = scontext.textFile("SalesJan2009.csv")
    val LineLength = distFile.map(s => s.length)
    val totalLength = LineLength.reduce((a, b) => a + b)
    print(totalLength)

  }
}
