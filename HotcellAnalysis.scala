package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import scala.math.sqrt
import java.lang.Math
import org.apache.spark.sql.types.{
    StructType, StructField, DoubleType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)


  def apply(any: Any): Double = 
    any match { case i: Int => i case f: Float => f case d: Double => d }

case class X(x: Double, y: Double, z: Double, zscore: Double)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var sum1 = 0.0
  var sum2 = 0.0 
  var sum3 = 0.0
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()
  val sc = spark.sparkContext
  val sqlContext = new SQLContext(sc)
  import spark.implicits._

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.createOrReplaceTempView("pickupinfo")
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  val average_x = pickupInfo.count()/numCells
  //println("average_x: "+average_x +", numcells: " +numCells+", numrecords: " + pickupInfo.count())
 
  val point_in_cell = spark.sql("select pickupinfo.x AS x,pickupinfo.y AS y,pickupinfo.z AS z, COUNT (pickupinfo.x, pickupinfo.y, pickupinfo.z) AS point from pickupinfo GROUP BY pickupinfo.x, pickupinfo.y, pickupinfo.z")
  point_in_cell.createOrReplaceTempView("point_in_cell")
  point_in_cell.show()

  
  val numPoints = point_in_cell.count()
  val sum_square = spark.sql("SELECT SUM(point * point) as sum_square FROM point_in_cell")
  val value = sum_square.first().getLong(0).toDouble
  val S = sqrt(value/numCells - average_x * average_x)

  val array = Array.ofDim[Double](numCells.toInt, 4)
  val schema = StructType(
    StructField("x", DoubleType, true) ::
    StructField("y", DoubleType, true) ::
    StructField("z", DoubleType, true) ::
    StructField("zscore", DoubleType, true) :: Nil)
  
  for (i <- 1 to numCells.toInt){
    var sum1 = 0.0
    var sum2 = 0.0
    var sum3 = 0.0
    println("processing No."+i+" cell..." )
    val cell_coordinate = HotcellUtils.get_cell_coordinate(i, minX, maxX, minY, maxY)
    val ix=cell_coordinate(0)
    val iy=cell_coordinate(1)
    val iz=cell_coordinate(2)

    val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
    if(get_number_of_cell.count() != 0){
      //self
      val cellj = get_number_of_cell.first().getLong(0).toDouble
      //println("self cell: " + cellj)
      sum1 = sum1 + cellj
      sum2 = sum2 + 1
      sum3 = sum3 + 1
      if(i > 1 && (HotcellUtils.cell_is_adjacent(i-1, i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-1, minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          //println("1 cell: " + cellj)
          sum1 = sum1 + cellj
        }
      }
      if(HotcellUtils.cell_is_adjacent(i+1, i, minX, maxX, minY, maxY, numCells) == 1){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+1, minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          //println("2 cell: " + cellj)
          sum1 = sum1 + cellj
        }
      }
      if(i > (maxX-minX+1) && (HotcellUtils.cell_is_adjacent(i-(maxX-minX+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-(maxX-minX+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          //println("3 cell: " + cellj)
          sum1 = sum1 + cellj
        }
      }
      if((HotcellUtils.cell_is_adjacent(i+(maxX-minX+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+(maxX-minX+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("4 cell: " + cellj)
        }
      }
      if(i > (maxX-minX+1 + 1) && (HotcellUtils.cell_is_adjacent(i-(maxX-minX+1 + 1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-(maxX-minX+1 + 1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("5 cell: " + cellj)
        }
      }
      if(i > (maxX-minX) && (HotcellUtils.cell_is_adjacent(i-(maxX-minX), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-(maxX-minX), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("6 cell: " + cellj)
        }
      }
      if((HotcellUtils.cell_is_adjacent(i+(maxX-minX+1+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+(maxX-minX+1+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("7 cell: " + cellj)
        }
      }
      if((HotcellUtils.cell_is_adjacent(i+(maxX-minX), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+(maxX-minX), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("8 cell: " + cellj)
        }
      }
      if((i > 1 + (maxX-minX+1)*(maxY-minY+1)) && (HotcellUtils.cell_is_adjacent(i-1-(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-1-(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("9 cell: " + cellj)
        }
      }
      if((i + 1 > (maxX-minX+1)*(maxY-minY+1))&&(HotcellUtils.cell_is_adjacent(i+1-(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+1-(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("10 cell: " + cellj)
        }
      }
      if((i > (maxX-minX+1)+(maxX-minX+1)*(maxY-minY+1) )&& (HotcellUtils.cell_is_adjacent(i-(maxX-minX+1)-(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-(maxX-minX+1)-(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("11 cell: " + cellj)
        }
      }
      if((i+(maxX-minX+1)>(maxX-minX+1)*(maxY-minY+1)) && (HotcellUtils.cell_is_adjacent(i+(maxX-minX+1)-(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+(maxX-minX+1)-(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("12 cell: " + cellj)
        }
      }
      if(i > (maxX-minX+1 + 1 + (maxX-minX+1)*(maxY-minY+1)) && (HotcellUtils.cell_is_adjacent(i-(maxX-minX+1 + 1)-(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-(maxX-minX+1 + 1)-(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("13 cell: " + cellj)
        }
      }
      if(i > (maxX-minX + (maxX-minX+1)*(maxY-minY+1)) && (HotcellUtils.cell_is_adjacent(i-(maxX-minX)-(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-(maxX-minX)-(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("14 cell: " + cellj)
        }
      }
      if((i+(maxX-minX+1+1)>(maxX-minX+1)*(maxY-minY+1))&&(HotcellUtils.cell_is_adjacent(i+(maxX-minX+1+1)-(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+(maxX-minX+1+1)-(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("15 cell: " + cellj)  
        }
      }
      if((i+(maxX-minX)>(maxX-minX+1)*(maxY-minY+1))&&(HotcellUtils.cell_is_adjacent(i+(maxX-minX)-(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+(maxX-minX)-(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("16 cell: " + cellj)  
        }
      }
      if((i>(maxX-minX+1)*(maxY-minY+1))&&(HotcellUtils.cell_is_adjacent(i-(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("17 cell: " + cellj)
        }
      }
      if((HotcellUtils.cell_is_adjacent(i+(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("18 cell: " + cellj)
        }
      }
      if(i > 1 && (HotcellUtils.cell_is_adjacent(i-1 + (maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-1 + (maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("19 cell: " + cellj)
        }
      }
      if(HotcellUtils.cell_is_adjacent(i+1+(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+1+(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("20 cell: " + cellj)
        }
      }
      if(i > (maxX-minX+1) && (HotcellUtils.cell_is_adjacent(i-(maxX-minX+1)+(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-(maxX-minX+1)+(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("21 cell: " + cellj)
        }
      }
      if((HotcellUtils.cell_is_adjacent(i+(maxX-minX+1)+(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+(maxX-minX+1)+(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("22 cell: " + cellj)
        }
      }
      if(i > (maxX-minX+1 + 1) && (HotcellUtils.cell_is_adjacent(i-(maxX-minX+1 + 1+(maxX-minX+1)*(maxY-minY+1)), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-(maxX-minX+1 + 1+(maxX-minX+1)*(maxY-minY+1)), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("23 cell: " + cellj)
        }
      }
      if(i > (maxX-minX) && (HotcellUtils.cell_is_adjacent(i-(maxX-minX)+(maxX-minX+1)*(maxY-minY+1), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i-(maxX-minX)+(maxX-minX+1)*(maxY-minY+1), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("24 cell: " + cellj)
        }
      }
      if((HotcellUtils.cell_is_adjacent(i+(maxX-minX+1+1+(maxX-minX+1)*(maxY-minY+1)), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+(maxX-minX+1+1+(maxX-minX+1)*(maxY-minY+1)), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("25 cell: " + cellj)
        }
      }
      if((HotcellUtils.cell_is_adjacent(i+(maxX-minX+(maxX-minX+1)*(maxY-minY+1)), i, minX, maxX, minY, maxY, numCells) == 1)){
        sum2 = sum2 + 1
        sum3 = sum3 + 1
        val cell_coordinate = HotcellUtils.get_cell_coordinate(i+(maxX-minX+(maxX-minX+1)*(maxY-minY+1)), minX, maxX, minY, maxY)
        val get_number_of_cell = spark.sql("select point_in_cell.point from point_in_cell where (point_in_cell.x=" + cell_coordinate(0)+" AND point_in_cell.y="+ cell_coordinate(1)+" AND point_in_cell.z="+ cell_coordinate(2)+")")
        if(get_number_of_cell.count() != 0){
          val cellj = get_number_of_cell.first().getLong(0).toDouble
          sum1 = sum1 + cellj
          //println("26 cell: " + cellj)
        }
      }
      //println("average x:" + average_x + " S:"+S + " numCells:"+numCells)
      //println("sum1:" + sum1 + " sum2:"+sum2 + " sum3:"+sum3)
      val g = (sum1 - average_x*sum2)/(S*sqrt((numCells*sum3-sum2*sum2)/(numCells-1)))
      array(i)(0) = ix
      array(i)(1) = iy
      array(i)(2) = iz
      array(i)(3) = g
    }
  }
  
  val rdd = sc.makeRDD(array)
  val dfFromArray = rdd.map { 
    case Array(s0, s1, s2, s3) => X(s0, s1, s2, s3)}.toDF()
  val newdf = dfFromArray.sort($"zscore".desc).drop($"zscore")
  newdf.createOrReplaceTempView("newdf")
  newdf.show(50)
  return newdf // YOU NEED TO CHANGE THIS PART
}
}
