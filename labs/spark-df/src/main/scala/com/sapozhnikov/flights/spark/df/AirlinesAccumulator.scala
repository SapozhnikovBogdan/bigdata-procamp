package com.sapozhnikov.flights.spark.df

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class AirlinesAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Int]]{
  private val hashmapAcc = new mutable.HashMap[String,Int]()
  // Check if the accumulator is empty
  override def isZero: Boolean = {
    hashmapAcc.isEmpty
  }
  //A copy of the accumulator returns a new accumulator
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new AirlinesAccumulator()
    hashmapAcc.synchronized {
      newAcc.hashmapAcc ++= hashmapAcc
    }
    newAcc
  }
  // Reset the accumulator, clear the data
  // After reset, call the isZero method to ensure that it returns true
  override def reset(): Unit = {
    hashmapAcc.clear()
  }
  //task call, processing each piece of data, data is added to the accumulator
  def add(airline:String) = {
    hashmapAcc.get(airline) match{
      case Some(v) => hashmapAcc +=((airline,v+1))
      case None => hashmapAcc +=((airline,1))
    }
  }
  // Combine the results of the accumulator in multiple partitions
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc:AccumulatorV2[String, mutable.HashMap[String, Int]] =>{
        for((k,v)<- acc.value){
          hashmapAcc.get(k) match {
            case Some(newv) => hashmapAcc +=((k,v+newv))
            case None => hashmapAcc +=((k,v))
          }
        }
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = hashmapAcc

}
