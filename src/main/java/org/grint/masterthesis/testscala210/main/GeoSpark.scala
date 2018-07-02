package org.grint.masterthesis.testscala210.main

import java.util

import com.vividsolutions.jts
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.Geometry
import org.wololo.geojson.{Geometry, Polygon,Point}
import com.vividsolutions.jts.util.GeometricShapeFactory
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.sql.{Encoders, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.grint.masterthesis.testscala210.loader.DataLoader
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD, SpatialRDD}
import org.datasyslab.geospark.{enums, spatialPartitioning}
import org.datasyslab.geosparksql.utils.Adapter
import org.geotools.geometry.jts.JTS
import org.wololo.geojson
import org.wololo.jts2geojson.GeoJSONWriter
import shapeless.PolyDefns.->

import scala.collection.mutable.ListBuffer
//import org.datasyslab.geosparksql.utils.Adapter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.RectangleRDD
import scala.collection.JavaConversions._
import org.apache.spark.sql.RowFactory





case class Out(minX:Double, maxX:Double, minY:Double, maxY:Double)

object GeoSpark {


  def partitioning(sparkSession: SparkSession) : Unit = {

    val conf = new SparkConf()

    conf.setAppName("Spark Hello World")
    conf.setMaster("local[2]")



    val sc = sparkSession.sparkContext
    import sparkSession.implicits._

    val InputLocation = "/Users/svetlana.matveeva/Documents/MasterThesis/Dataset/test22.csv"//DataLoader.cardsDF.col("applicantlocation")
    val Offset = 0 // The WKT string starts from Column 0
    val Splitter = FileDataSplitter.CSV//GEOJSON
    val gridType = GridType.EQUALGRID
    val numPartitions = 60
    val carryOtherAttributes = true

   var pointRDD: PointRDD = new PointRDD (sc,InputLocation,Offset,Splitter, carryOtherAttributes, numPartitions)
    //val res = spatialRDD.spatialPartitionedRDD(GridType.KDBTREE)
    pointRDD.analyze()

    val res2 = pointRDD.spatialPartitioning(gridType)
    val grid = pointRDD.grids.listIterator()
    var col = new ListBuffer[Out]()

    val value = sc.parallelize(pointRDD.grids.toList).map((f: Envelope) => Out(f.getMinX, f.getMaxX, f.getMinY, f.getMaxY))
    println(pointRDD.grids.length)
    value.foreach(p => println(p))
      var valueDF = value.toDF("minX", "maxX", "minY", "maxY")

    valueDF.coalesce(1).write
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .csv("output.csv")


    var valuePL = value.map(f=> {
      val gsf = new GeometryFactory()
      val coordinates = new Array[Coordinate](5)
      coordinates(0) = new Coordinate(f.minX,f.minY)
      coordinates(1) = new Coordinate(f.maxX,f.minY)
      coordinates(2) = new Coordinate(f.minX,f.maxY)
      coordinates(3) = new Coordinate(f.maxX,f.maxY)
      coordinates(4) = coordinates(0)
      val polObj = gsf.createPolygon(coordinates)
      polObj }
    )


println(valuePL.count())
    valuePL.foreach(f=> println(f.getCoordinate.x))
    //queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
    val p = new PolygonRDD(valuePL.toJavaRDD())
    p.spatialPartitioning(pointRDD.getPartitioner)
    //val result = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, usingIndex, considerBoundaryIntersection)
    val res1 = JoinQuery.SpatialJoinQuery(pointRDD,p,false,true)
println(res1.count())
    val res3 = JoinQuery.SpatialJoinQueryFlat(pointRDD,p,false,true)
    val res4 = JoinQuery.SpatialJoinQueryWithDuplicates(pointRDD,p,false,true)
    //res1.coalesce(1).saveAsTextFile("/Users/svetlana.matveeva/Documents/MasterThesis/Dataset/test.txt")
//res3.coalesce(1).saveAsTextFile("/Users/svetlana.matveeva/Documents/MasterThesis/Dataset/test1.txt")
    //res4.coalesce(1).saveAsTextFile("/Users/svetlana.matveeva/Documents/MasterThesis/Dataset/test2.txt")
    //JavaRDD<Row> rowRDD = filesRDD.map(tuple -> RowFactory.create(tuple._1(),tuple._2()));

    //val rowRDD = res3.map((f) => RowFactory.create(f._1, f._2))

    //val value = sc.parallelize(pointRDD.grids.toList).map((f: Envelope) => Out(f.getMinX, f.getMaxX, f.getMinY, f.getMaxY))
    //val df = sparkSession.createDataset(JavaPairRDD.toRDD(res3), Encoders.tuple(Encoders.STRING, Encoders.STRING)).toDF
    //val valueRes = res3.rdd.toDF().coalesce(1).write.save//.toJavaRDD().rdd.toDF().schema
    //var joinResultDf = Adapter.toDf(res3,sparkSession)



  }



}
