package com.adp.revenue.contribution

import org.apache.spark.sql.SparkSession

object RevenueContribution {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Revenue Contribution Calculator")
      .master("local")
      .getOrCreate()

    val data = spark.read.text("/home/raja/Desktop/Dataset/ga.csv/ga.csv",4)
    val validData = data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val id_country_revenue = validData.map(item => (getFullVersionId(item),getCountry(item),getTransactionRevenue(item)))
    val distinctIds = id_country_revenue.map(item => item._1).distinct.count
    val distinctCountries = id_country_revenue.map(item => item._2).distinct.count
    val totalRevenue = id_country_revenue.map(item => if(item == "") 0 else item._3.toLong).distinct.sum

    val revenueByUser = totalRevenue/distinctIds
    val revenueByCountry = totalRevenue/distinctCountries
    spark.stop();
  }

  def getFullVersionId(item : String) : String = {
    if(item.indexOf("\"\"}\"") == -1 || item.indexOf("\"{\"\"continent") == -1) "" else item.substring(item.indexOf("\"\"}\"")+5,item.indexOf("\"{\"\"continent")-2)
  }

  def getCountry(item : String) : String = {
    if(item.indexOf("\"\"country\"\"") == -1 || item.indexOf("\"\", \"\"region\"\"") == -1) "" else item.substring(item.indexOf("\"\"country\"\"")+15,item.indexOf("\"\", \"\"region\"\""))
  }

  def getTransactionRevenue(item : String) : String = {
    if(item.indexOf("transactionRevenue") == -1 || item.indexOf("\"\", \"\"newVisits") == -1) "" else item.substring(item.indexOf("transactionRevenue")+24,item.indexOf("\"\", \"\"newVisits"))
  }

}
