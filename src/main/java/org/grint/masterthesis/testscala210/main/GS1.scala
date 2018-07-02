package org.grint.masterthesis.testscala210.main


import com.cloudera.sparkts.models.ARIMA
import com.vividsolutions.jts
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.util.GeometricShapeFactory
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.geosparksql.expressions.{ST_Point, ST_PolygonFromEnvelope, ST_PolygonFromText}
import org.apache.spark.sql.{Dataset, Encoders, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.grint.masterthesis.testscala210.loader.DataLoader
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD, SpatialRDD}
import org.datasyslab.geospark.{enums, spatialPartitioning}
import org.datasyslab.geosparksql.utils.Adapter
import org.geotools.geometry.jts.JTS
import org.wololo.geojson
import org.wololo.jts2geojson.GeoJSONWriter
import shapeless.PolyDefns.->
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparksql.UDF
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
//import org.datasyslab.geosparksql.utils.Adapter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.RectangleRDD
import scala.collection.JavaConversions._
import org.apache.spark.sql.RowFactory
import org.datasyslab.geospark.utils.GeoSparkConf


import org.apache.spark.serializer.KryoSerializer

object GS1 {


  def partitioning1(sparkSession: SparkSession) : Unit = {

    GeoSparkSQLRegistrator.registerAll(sparkSession)
   // GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)
    //GeoSparkSQLRegistrator.registerAll(sparkSession)
    val conf = new SparkConf()

    conf.setAppName("Spark Hello World")
    conf.setMaster("local[2]")



    val sc = sparkSession.sparkContext
    import sparkSession.implicits._

    //import Point data from csv
//    val pointWktDF  = sparkSession.read.format("csv")
//      .option("header", "true")
//      .option("delimiter", ",")
//      .option("nullValue", "")
//      .option("treatEmptyValuesAsNulls", "true")
//      .load("/Users/svetlana.matveeva/Documents/MasterThesis/Dataset/DataNew15.csv")
//    println(pointWktDF.count())
//    pointWktDF.createOrReplaceTempView("pointtable")

    val pointWktDF= sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/callcenterdb?user=postgres&password=123")
      .option("dbtable",
        "(select t2.id, t2.addresstext, t1.longitude, t1.latitude, t2.createddatetime from callcenter.address_with_gps as t1 left join callcenter.cards as t2 on t1.id=t2.id limit 500)as t1")
      .option("columnname", "id, addresstext,longitude,latitude,createddatetime")
      .option("user", "postgres")
      .option("password", "123")
      .load()


    pointWktDF.createOrReplaceTempView("pointtable")
    println(pointWktDF.count())

    //create PointDF
    val pointDF= sparkSession.sql("select ST_Point(cast(latitude as Decimal(24,20)), cast(longitude as Decimal(24,20)), cast(id as String),cast(addresstext as String), cast(createddatetime as String)) as area from pointtable")//,id,createddatetime,addresstext  from pointtable")
    println(pointDF.count())
   pointDF.printSchema()

    //create PointRDD
    val pointRDD = new SpatialRDD[Geometry]
    //pointRDD.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String])
    pointRDD.rawSpatialRDD = Adapter.toRdd(pointDF)
    //pointRDD.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String])
    pointRDD.analyze()
    //pointRDD.rawSpatialRDD.coalesce(1).saveAsTextFile("/Users/svetlana.matveeva/Documents/MasterThesis/Dataset/PointRDD")

    //import Polygon data from csv
   val polygonWktDF = sparkSession.read.format("csv")
    .option("header", "true")
     .option("delimiter", ",")
     .option("nullValue", "")
     .option("treatEmptyValuesAsNulls", "true")
     .load("/Users/svetlana.matveeva/IdeaProjects/TestScala210/output.csv")

   val polygonID=polygonWktDF.withColumn("ID",monotonically_increasing_id())


   polygonID.createOrReplaceTempView("polygontable")
    //create PolygonDF
    val polygonDF= sparkSession.sql("select ST_PolygonFromEnvelope(cast(minX as Decimal(24,20)), cast(minY as Decimal(24,20)), cast(maxX as Decimal(24,20)), cast(maxY as Decimal(24,20)),cast(ID as String) ) from polygontable")
    //create PolygonRDD
    val polygonRDD = new SpatialRDD[Geometry]
    polygonRDD.rawSpatialRDD = Adapter.toRdd(polygonDF)
    polygonRDD.analyze()

    // SPartitioning of PointRDD and PolygonRDD
    pointRDD.spatialPartitioning(GridType.EQUALGRID)
    polygonRDD.spatialPartitioning(pointRDD.getPartitioner)

    //val rddWithOtherAttributes = objectRDD.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String])

    //Join
    val joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD,polygonRDD,false,false)
    //val rddwithotherattr = joinResultPairRDD.rdd.map[String](f=>f.g)
    //joinResultPairRDD.rdd.map(f=>f._2.getUserData.asInstanceOf[String])
    val joinResultDf= Adapter.toDf(joinResultPairRDD,sparkSession)//.schema("polygon", "point","id","addresstext","createddatetime")
    println(joinResultDf.count())
    joinResultDf.coalesce(1).write.csv("/Users/svetlana.matveeva/Documents/MasterThesis/Dataset/joinresult")
    joinResultDf.schema
    joinResultDf.printSchema()

//
//    // Arima model
//    joinResultDf.createOrReplaceTempView("joinResultView")
//    val inputdata= sparkSession.sql("select cast(count(_c2)as DOUBLE) as count, cast(_c4 as DATE) as createddatetime from joinResultView group by cast(_c4 as DATE)")
//    inputdata.coalesce(1).write.csv("/Users/svetlana.matveeva/Documents/MasterThesis/Dataset/inputdata")
//    val inputdata1= Vectors.dense(inputdata.select("count").collect().map(_.getDouble(0)))
//    val amodel1= ARIMA.fitModel(1,0,1,inputdata1)//ARIMA.fitModel(1,0,1,inputdata1)
//    println("coefficients: " + amodel1.coefficients.mkString(","))
//    val forecast = amodel1.forecast(inputdata1, 10)
//    println("forecast of next 3 observations: " + forecast.toArray.mkString(","))
//
//
//    val ts = Vectors.dense(Array(6.0, 7.0, 8.0,6.0,8.0))
//    //              val arimaModel = ARIMA.fitModel(1,0,1,ts)
//    val arimaModel = ARIMA.autoFit(ts)
//    val valueList = arimaModel.forecast(ts, 1)
//    arimaModel
//    println("forecast of next 3 observations: " + valueList.toArray.mkString((",")))
  }


}
