package org.grint.masterthesis.testscala210.main


import com.vividsolutions.jts
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.util.GeometricShapeFactory
import org.apache.spark.api.java.JavaPairRDD
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
        "(select t2.id, t2.addresstext, t1.longitude, t1.latitude, t2.createddatetime from callcenter.address_with_gps as t1 left join callcenter.cards as t2 on t1.id=t2.id limit 15)as t1")
      .option("columnname", "id, addresstext,longitude,latitude,createddatetime")
      .option("user", "postgres")
      .option("password", "123")
      .load()


    //val rddWithOtherAttributes = objectRDD.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String])
    pointWktDF.createOrReplaceTempView("pointtable")
    println(pointWktDF.count())

    //create PointDF
    val pointDF= sparkSession.sql("select ST_Point(cast(latitude as Decimal(24,20)), cast(longitude as Decimal(24,20))) as area,id,addresstext, createddatetime from pointtable")//,id,createddatetime,addresstext  from pointtable")
    println(pointDF.count())
    //create PointRDD
    val pointRDD = new SpatialRDD[Geometry]
    //pointRDD.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String])
    pointRDD.rawSpatialRDD = Adapter.toRdd(pointDF)
    pointRDD.analyze()
    pointRDD.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String])

    //import Polygon data from csv
   val polygonWktDF = sparkSession.read.format("csv")
    .option("header", "true")
     .option("delimiter", ",")
     .option("nullValue", "")
     .option("treatEmptyValuesAsNulls", "true")
     .load("/Users/svetlana.matveeva/IdeaProjects/TestScala210/output.csv")


   polygonWktDF.createOrReplaceTempView("polygontable")
    //create PolygonDF
    val polygonDF= sparkSession.sql("select ST_PolygonFromEnvelope(cast(minX as Decimal(24,20)), cast(maxX as Decimal(24,20)), cast(minY as Decimal(24,20)), cast(maxY as Decimal(24,20))) from polygontable")
    //create PolygonRDD
    val polygonRDD = new SpatialRDD[Geometry]
    polygonRDD.rawSpatialRDD = Adapter.toRdd(polygonDF)
    polygonRDD.analyze()

    // SPartitioning of PointRDD and PolygonRDD
    pointRDD.spatialPartitioning(GridType.EQUALGRID)
    polygonRDD.spatialPartitioning(pointRDD.getPartitioner)

    //val rddWithOtherAttributes = objectRDD.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String])

    //Join
    val joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD,polygonRDD,false,true)
    //val rddwithotherattr = joinResultPairRDD.rdd.map[String](f=>f.g)
    val joinResultDf= Adapter.toDf(joinResultPairRDD,sparkSession)
    println(joinResultDf.count())
    joinResultDf.coalesce(1).write.csv("/Users/svetlana.matveeva/Documents/MasterThesis/Dataset/joinresult")
  }
}
