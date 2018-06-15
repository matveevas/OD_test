package org.grint.masterthesis.testscala210.main

import java.util

import com.vividsolutions.jts.geom.{Coordinate, Geometry}
import org.wololo.geojson.{Geometry, Polygon}
import com.vividsolutions.jts.util.GeometricShapeFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.grint.masterthesis.testscala210.loader.DataLoader
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD, SpatialRDD}
import org.datasyslab.geospark.{enums, spatialPartitioning}
import org.geotools.geometry.jts.JTS
import org.wololo.jts2geojson.GeoJSONWriter
//import org.datasyslab.geosparksql.utils.Adapter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialOperator.JoinQuery.DistanceJoinQuery
import org.datasyslab.geospark.spatialOperator.JoinQuery.DistanceJoinQueryCountByKey


case class Out(clusterId:Int, id:String, longitude:Double, latitude:Double, timestamp:Long)

object GeoSpark {


  def partitioning(sparkSession: SparkSession) : Unit = {

    val conf = new SparkConf()

    conf.setAppName("Spark Hello World")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

   val InputLocation = "svetlana.matveeva/Documents/MasterThesis/arealmsmall.csv"//DataLoader.cardsDF.col("applicantlocation")
    val Offset = 0 // The WKT string starts from Column 0
    val Splitter = FileDataSplitter.CSV//GEOJSON
    val gridType = GridType.KDBTREE
    val numPartitions = 200
    val carryOtherAttributes = true

   var pointRDD: PointRDD = new PointRDD (sc,InputLocation,Offset,Splitter, carryOtherAttributes, numPartitions)
    //val res = spatialRDD.spatialPartitionedRDD(GridType.KDBTREE)
    val res2 = pointRDD.spatialPartitioning(gridType)


  }
  /*   Save Partitions Grids Boundaries as GeoJSON  */
  def saveAsGeoJSONBoundaries(sc:SparkContext,pointRDD:PointRDD,output:String): Unit = {

    val g = pointRDD.grids.listIterator()
    val writer = new GeoJSONWriter()
    val result: util.ArrayList[String] = new util.ArrayList[String]

    while (g.hasNext) {
      val item = g.next()
      //println(item.grid, " ", item.getArea)
      println(item, " ", item.getMinX, item.getMinY, item.getMaxX, item.getMaxY)
      val gsf = new GeometricShapeFactory();
      gsf.setBase(new Coordinate(item.getMinX, item.getMinY))
      gsf.setWidth(item.getMaxX - item.getMinX)
      gsf.setHeight(item.getMaxY - item.getMinY)
      gsf.setNumPoints(4);
      val polygon = gsf.createRectangle()
     // JTS.transform()
      //var p = new Polygon(polygon.getCoordinates.)
      //val gson = writer.write(polygon).toString + ","
      //result.add(gson)



    }
    //sc.parallelize(result.toArray()).coalesce(1).saveAsTextFile(output)
  }
}
