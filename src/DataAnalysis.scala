/**
  * Created by mathiasedouin on 13/01/17.
  */

import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

import com.github.nscala_time._
import com.github.nscala_time.time._

import java.sql.Timestamp

object DataAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("DAdemo").setMaster("local[2]")
    val scontext = new SparkContext(sparkconf)
    val rootlogger = Logger.getRootLogger().setLevel(Level.ERROR)
    val sqlcontext = new SQLContext(scontext)

    val temp = sqlcontext.read.option("header","true").csv("SalesJan2009.csv")
    val df = temp.selectExpr(
      "cast(Transaction_date as Timestamp) Transaction_date",
      "Product",
      "cast(Price as int) Price",
      "Payment_Type",
      "Name",
      "City",
      "State",
      "Country",
      "cast(Account_Created as date) Account_Created",
      "cast(Last_Login as date) Last_Login",
      "cast(Latitude as float) Latitude",
      "cast(Longitude as float) Longitude")

    df.printSchema()
    df.select("Transaction_date").show()

    /* NUMBER OF PAYMENTS */
    println("---------------------------\n")
    println("NUMBER OF DIFFERENT PAYMENTS")
    val PaymentNumb  = df.select("Payment_Type").distinct().count()
    println(s"There is $PaymentNumb different types of payment")
    println("---------------------------\n\n")



    /* SALES - FIRST HALF OF THE MONTH */
    println("\nSALES FOR THE FIRST HALF OF THE MONTH")


    val prod = df.select("Product")
      .filter(s"Transaction_date >= 1/1/2009")
      .filter(s"Transaction_date <= 1/15/2009")
      .count()

    println(s"There is $prod sold")
    print("---------------------------\n")
  }

}
