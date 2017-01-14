/**
  * Created by mathiasedouin on 13/01/17.
  */

import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.types._
import com.github.nscala_time._


object DataAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("DAdemo").setMaster("local[2]")
    val scontext = new SparkContext(sparkconf)
    val rootlogger = Logger.getRootLogger().setLevel(Level.ERROR)
    val sqlcontext = new SQLContext(scontext)

    val format = new java.text.SimpleDateFormat("mm/dd/yyyy")
    format.format(new java.util.Date())

    val temp = sqlcontext.read.option("header","true").csv("SalesJan2009.csv")
    val df = temp.selectExpr(
      "cast(Transaction_date as date) Transaction_date",
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

    /* NUMBER OF PAYMENTS */
    println("---------------------------\n")
    println("NUMBER OF DIFFERENT PAYMENTS")
    val PaymentNumb  = df.select("Payment_Type").distinct().count()
    println(s"There is $PaymentNumb different types of payment")
    println("---------------------------\n\n")



    /* SALES - FIRST HALF OF THE MONTH */
    println("\nSALES FOR THE FIRST HALF OF THE MONTH")

    val date = ("1/1/2009").toString(Static)

    df.select("Product").filter(
      "Transaction_date" >= fmt
    )
    print("---------------------------\n")
  }

}
