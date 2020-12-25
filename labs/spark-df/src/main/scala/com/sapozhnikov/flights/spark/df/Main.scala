package com.sapozhnikov.flights.spark.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, when, round, broadcast}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName(" canceled flights statistics").getOrCreate()

    //Gather total number of flights per airline for debbuging
    // Create an accumulator, then register
    val airlinesAccumulator = new AirlinesAccumulator()
    spark.sparkContext.register(airlinesAccumulator,"Airlines Accumulator")

    import spark.implicits._

    val dfAirlines = spark.read.option("sep",",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/user/bogdan/airlines.csv")

    val dfAirports = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/user/bogdan/airports.csv")
      .select($"IATA_CODE".alias("AIRPORT_CD"), $"AIRPORT")

    val dfFlights = spark.read.option("sep",",").option("inferSchema", "true").option("header", "true")
      .csv("/user/bogdan/flights.csv")
      .select($"AIRLINE",$"ORIGIN_AIRPORT",$"CANCELLED")
      .groupBy($"ORIGIN_AIRPORT", $"AIRLINE")
      .agg(count(when($"CANCELLED"===1, 1)).alias("CANCELLED_CNT"), count($"CANCELLED").alias("CNT"))
      .select($"AIRLINE".alias("AIRLINE_CD"),$"ORIGIN_AIRPORT",round($"CANCELLED_CNT"/$"CNT"*100, 2).alias("CANCELLED_PERCENTAGE"), $"CANCELLED_CNT", $"CNT")
      .join(broadcast(dfAirlines),$"AIRLINE_CD"===$"IATA_CODE")
      .join(broadcast(dfAirports),$"ORIGIN_AIRPORT"===$"AIRPORT_CD")
      .selectExpr("AIRLINE as AIRLINE_NAME", "AIRPORT as ORIGIN_AIRPORT_NAME", "CANCELLED_PERCENTAGE", "CANCELLED_CNT", "CNT")
      .persist()

    val dfWRA = dfFlights.where("ORIGIN_AIRPORT_NAME = 'Waco Regional Airport'")
      .write.csv("/user/bogdan/Waco_Regional_Airport_CSV")

    val dfFlightsRest = dfFlights.sort($"AIRLINE_NAME", $"CANCELLED_PERCENTAGE")
      .write.mode("append").json("/user/bogdan/flights_canceled_json")

    val rddFlights = dfFlights.rdd.map(row => row.getAs("AIRLINE_NAME").toString).foreach(v => airlinesAccumulator.add(v))

    airlinesAccumulator.value.foreach{ case(key, value) => println(key + " -> " + value)}

    spark.stop()
  }
}
