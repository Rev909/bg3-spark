/**
  * Created by mathiasedouin on 13/01/17.
  */

import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.types._

object DataAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("DAdemo").setMaster("local[2]")
    val scontext = new SparkContext(sparkconf)
    val rootlogger = Logger.getRootLogger().setLevel(Level.ERROR)
    val sqlcontext = new SQLContext(scontext)

    import sqlcontext.implicits._

    case class Sale(Transaction_date: Date,
                    Price: Int, Payment_Type: String, Name: String,
                    City: String, State: String, Country: String,
                    Account_Created: Date, Last_Login: Date, Latitude: Double, Longitude: Double)

    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("mm/dd/yyyy HH:MM")

    //UPLOAD THE FILE INTO SPARK
    val SalesJan2009 = scontext.textFile("SalesJan2009.csv")
      .map(_.split(","))
      .map(value => Sale(simpleDateFormat.parse(value(0)), value(1).trim.toInt, value(2), value(3), value(4).trim.toLowerCase(),
        value(5), value(6), simpleDateFormat.parse(value(7)), simpleDateFormat.parse(value(8)), value(9).trim.toDouble, value(10).trim.toDouble))

    val df = sqlcontext.createDataFrame(SalesJan2009, )

    /* NUMBER OF PAYMENTS */
    println("---------------------------\n")
    println("NUMBER OF DIFFERENT PAYMENTS")
    SalesJan2009.toDS()
    print("---------------------------\n")

    /* SALES - FIRST HALF OF THE MONTH */
    println("\nSALES FOR THE FIRST HALF OF THE MONTH")
    print("---------------------------\n")
  }

}
