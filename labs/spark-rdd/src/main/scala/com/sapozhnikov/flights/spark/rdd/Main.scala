package com.sapozhnikov.flights.spark.rdd
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("flights_destination_airports").getOrCreate()

    val sc = spark.sparkContext
    val rddAirports =sc.textFile("airports.csv").map(s => {val arr=s.split(","); (arr(0),arr(1))})
    val rddFlights = sc.textFile("flights.csv")
    rddFlights.mapPartitionsWithIndex((indx, iter) => if (indx==0) iter.drop(1) else iter).map(s => s.split(","))
      .map(da=>(da(1), da(8))) //get month and airport code
      .map(da => (da, 1)) // add 1 for further counting
      .reduceByKey((cur, nxt) => cur + nxt) //count by "month_airport_code" key
      .map{case(k, v) => (k._1, (k._2, v))} //new key-value: key=month value=(airport_code, count(airport_code))
      .reduceByKey{case (cur, nxt) => if (cur._2 > nxt._2) cur else nxt} //getting max count(airport_code) by month
      .map{case(k,v) => (v._1, (k, v._2))} //new key-value: key=airport_code value=(month, count(airport_code))
      .join(rddAirports) //join with Airport dataset by airport_code
      .map{case(k, v) => (v._1._1.toInt, v._2 + "\\t" + v._1._2)} //new key-value: key=month value=(airport_code, count(airport_code))
      .sortByKey()
      .map{case(k, v) => k + "\\t" + v}
      .saveAsTextFile("/output")

    spark.stop()
  }
}
