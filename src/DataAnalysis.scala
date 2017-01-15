/**
  * Created by mathiasedouin on 13/01/17.
  */

import java.sql.Timestamp

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat

import java.text.SimpleDateFormat

object DataAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("DAdemo").setMaster("local[2]")
    val scontext = new SparkContext(sparkconf)
    val rootlogger = Logger.getRootLogger().setLevel(Level.ERROR)
    val sqlcontext = new SQLContext(scontext)

    val temp = sqlcontext.read.option("header","true").csv("SalesJan2009.csv")
    val df = temp.selectExpr(
      "Transaction_date",
      "Product",
      "cast(Price as int) Price",
      "Payment_Type",
      "Name",
      "City",
      "State",
      "Country",
      "Account_Created",
      "Last_Login",
      "Latitude",
      "Longitude")

    val ts = unix_timestamp(df.col("Transaction_date"),"MM/dd/yyyy hh:mm")
      .cast("double")
      .cast("timestamp")
    val ac = unix_timestamp(df.col("Account_Created"),"MM/dd/yyyy hh:mm")
      .cast("double")
      .cast("timestamp")
    val ll = unix_timestamp(df.col("Last_Login"),"MM/dd/yyyy hh:mm")
      .cast("double")
      .cast("timestamp")
    df.drop("Transaction_date").drop("Account_Created").drop("Last_Login")
    var df2 = df.withColumn("Transaction_date", ts).withColumn("Account_Created", ac).withColumn("Last_login", ll)

    df2.createOrReplaceTempView("sales")

    /* NUMBER OF PAYMENTS */
    println("\n---------------------------\n")
    println("NUMBER OF DIFFERENT PAYMENTS")
    val PaymentNumb  = df.select("Payment_Type").distinct().count()
    println(s"There is $PaymentNumb different types of payment")
    println("---------------------------\n")


    /* SALES - FIRST HALF OF THE MONTH */
    println("SALES FOR THE FIRST HALF OF THE MONTH")
    val datedeb = "0009-01-01"
    val datefin = "0009-01-15"
    val SalesHMonth =  df2.select("Product").where(s"Transaction_date BETWEEN CAST('$datedeb' AS TIMESTAMP) AND CAST('$datefin' AS TIMESTAMP)").count()
    println(s"There is $SalesHMonth products sold between $datedeb and $datefin")
    println("---------------------------\n")

    /* PRODUCT 1 */
    println("PRODUCT 1 : MORE VISA OR MASTERCARD ?")
    df2.select("Payment_Type").where("Product = 'Product1'").groupBy("Payment_Type").count().orderBy(desc("count")).show()
    println("---------------------------\n")

    /* AVG ACCOUNT_CREATED -> FIRST PURCHASE */
    println("AVG TIME BETWEEN ACCOUNT CREATION AND PURCHASE")
    val diffdate = datediff(df2.col("Transaction_date"), df2.col("Account_Created"))
    val AvgDaysDF = df2.withColumn("DateDiff", diffdate).agg(mean("DateDiff")).collect()
    var AvgDays = "Hello"
    for (x <- AvgDaysDF) {
      AvgDays = x.toString().replace("[","").replace("]","")
    }
    println(s"On average there is ${AvgDays.toDouble.toInt} days passing between account creation and first purchase")
    println("---------------------------\n")
   
    //Q6
    df.select("Product", "Price").groupBy("Product").sum("Price").show()

    //Q7
    val df3 = df.select(df("Product"), df("Price") + 2000)
    df3.show()
    df3.write.format("com.databricks.spark.csv").save("myFile.csv")

    //Q8
    df.select("Latitude", "Longitude").groupBy("Latitude", "Longitude").count().orderBy("count").filter("count > 2").show()
  }

}
