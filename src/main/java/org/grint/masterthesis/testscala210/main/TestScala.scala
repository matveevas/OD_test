package org.grint.masterthesis.testscala210.main

import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.grint.masterthesis.testscala210.loader.DataLoader

object TestScala {
  def main(args: Array[String]): Unit = {


    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()


    //      val plot = Vegas("Country Pop").
    //        withDataFrame(DataLoader.criminalsDF).
    //        encodeX("datetime", Nom).
    //        encodeY("count", Quant).
    //        encodeSize("Horsepower", Quant).
    //        encodeColor("Original", Nominal).
    //        mark(Line)
    //
    //plot.show
    DataLoader.load(sparkSession)
    val ts= Vectors.dense(DataLoader.criminalsDF.select("count").collect().map(_.getDouble(0)))
    //println(ts)
    val amodel= ARIMA.fitModel(1,0,1,ts)
    println("coefficients: " + amodel.coefficients.mkString(","))
    val forecast = amodel.forecast(ts, 20)
    println("forecast of next 20 observations: " + forecast.toArray.mkString(","))

  }

}
