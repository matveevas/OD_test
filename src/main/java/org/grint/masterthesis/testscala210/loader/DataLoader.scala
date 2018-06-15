package org.grint.masterthesis.testscala210.loader

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DataLoader {

  var criminalsDF:DataFrame = _
  var cardsDF:DataFrame=_

  // unit ничего не возвращает
  def load(sparkSession: SparkSession): Unit ={

    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/callcenterdb?user=postgres&password=123")
      .option("dbtable", "callcenter.cardcriminals")
      .option("user", "postgres")
      .option("password", "123")
      .load()

    jdbcDF.show(10)

    cardsDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/callcenterdb?user=postgres&password=123")
      .option("dbtable",
        "(select t2.id, t2.createddatetime,t2.addresstext,t2.applicantlocation, t1.longitude, t1.latitude from callcenter.address_with_gps as t1 left join callcenter.cards as t2 on t1.id=t2.id)as t1")
      .option("columnname", "id, createddatetime, addresstext, applicantlocation,latitude,longitude")
      .option("user", "postgres")
      .option("password", "123")
      .load()

    cardsDF.select("id", "createddatetime", "addresstext").show(numRows = 20)

      criminalsDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/callcenterdb?user=postgres&password=123")
      .option("dbtable",
        //"(select id, addresscode, addresstext, date_trunc('day',datetime) as datetime from callcenter.cards where id in (select cardid from callcenter.cardsufferers ) and addresstext like '%Казань%' order by id)AS t")
        "( select cast(count(id) as double PRECISION) as count ,date_trunc('day',datetime) as datetime from callcenter.cards where id in (select cardid from callcenter.cardsufferers ) and addresstext like '%Казань%' group by date_trunc('day',datetime)) as t")
      .option("columnname", "count, datetime")
      .option("columnntype", "double, datetime")
      .option("user", "postgres")
      .option("password", "123")
      .load()
    //val criminalsDF1= criminalsDF.groupBy("datetime").count().show(numRows = 30)

    criminalsDF.select("count", "datetime").show(numRows = 15)

  }

}
