package org.grint.masterthesis.testscala210.main

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.grint.masterthesis.testscala210.loader.DataLoader
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD, SpatialRDD}
import org.datasyslab.geospark.spatialPartitioning
import org.datasyslab.geosparksql.utils.Adapter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialOperator.JoinQuery.DistanceJoinQuery
import org.datasyslab.geospark.spatialOperator.JoinQuery.DistanceJoinQueryCountByKey




object GeoSpark {


  def partitioning(sparkSession: SparkSession) : Unit = {

    val conf = new SparkConf()

    conf.setAppName("Spark Hello World")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

//    val pointRDDInputLocation = DataLoader.cardsDF.col("applicantlocation")
//    val pointRDDOffset = 0 // The WKT string starts from Column 0
//    val pointRDDSplitter = FileDataSplitter.GEOJSON
//    val carryOtherAttributes = true
//    var objectRDD = new PointRDD(sparkSession, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, carryOtherAttributes)
//    var gsDF = DataLoader.cardsDF.select("applicantlocation")
//    var spatialRDD =new SpatialRDD[Geometry]
//    spatialRDD.rawSpatialRDD =Adapter.toRdd(gsDF)
//
//    println(spatialRDD)
//    val res = spatialRDD.spatialPartitionedRDD(GridType.KDBTREE)
//    "/home/SparkUser/Downloads/GeoSpark/src/test/resources/arealm.csv"
//    val queryWindowRDD = new PolygonRDD(sparkSession, "svetlana.matveeva/Downloads/arealm.csv", 0, PolygonRDDEndOffset, PolygonRDDSplitter, true)
//    .spatialPartitioning(spatialRDD.getPartitioner)
//
//    val result = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, usingIndex, considerBoundaryIntersection)
//
//
//
//   // gsDF.take(10).foreach(println)
//val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"

    val PointRDDInputLocation = "svetlana.matveeva/Documents/MasterThesis/arealmsmall.csv"
    val PointRDDSplitter = FileDataSplitter.CSV
    val PointRDDIndexType = IndexType.RTREE
    val PointRDDNumPartitions = 5
    val PointRDDOffset = 1

    val PolygonRDDInputLocation = "svetlana.matveeva/Documents/MasterThesis/primaryroads-polygon.csv"
    val PolygonRDDSplitter = FileDataSplitter.CSV
    val PolygonRDDIndexType = IndexType.RTREE
    val PolygonRDDNumPartitions = 5
    val PolygonRDDStartOffset = 0
    val PolygonRDDEndOffset = 9

    val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    objectRDD.analyze()
    objectRDD.spatialPartitioning(GridType.QUADTREE)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
    val eachQueryLoopTimes = 1
    for (i <- 1 to eachQueryLoopTimes) {
     //val resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, false, true).count()
      //val res2 = JoinQuery.distanceJoin(objectRDD, queryWindowRDD, false, true).count()
    }

    //val resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, false, true).count()
    //val res2= JoinQuery.distanceJoin(objectRDD, queryWindowRDD, false, true).count()

  }
}
