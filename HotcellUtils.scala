package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.math.min
import scala.math.max
import scala.math._

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def cell_is_adjacent(cell_a:Double, cell_b:Double, minX:Double, maxX:Double, minY:Double, maxY:Double, numCells:Double): Double = {
   if (cell_a < 1 || cell_b < 1)
    return 0
   if(cell_a > numCells || cell_b > numCells)
    return 0
   val a_coordinate = get_cell_coordinate(cell_a, minX, maxX, minY, maxY)
   val b_coordinate = get_cell_coordinate(cell_b, minX, maxX, minY, maxY)
   if(abs(a_coordinate(0)-b_coordinate(0)) < 2){
    if(abs(a_coordinate(1)-b_coordinate(1)) < 2){
      if(abs(a_coordinate(2)-b_coordinate(2)) < 2){
        return 1
      }
    }
   }
   return 0

  }

  def get_cell_number(x: Double, y: Double, z: Double, minX: Double, maxX: Double, minY:Double, maxY:Double):Double={
   return ((z - 1) * (maxX - minX + 1) * (maxY - minY + 1) + (maxY - y) * (maxX - minX + 1) + (x - minX + 1))
  }

  def get_cell_coordinate(i:Double, minX: Double, maxX: Double, minY:Double, maxY:Double): Array[Int]={
    val coordinate = new Array[Int](3)
   coordinate(2) = (Math.floor (i / ((maxY - minY + 1) * (maxX - minX + 1) + 1)) + 1).toInt
   val new_i = i - (coordinate(2) - 1) * ((maxY - minY + 1) * (maxX - minX + 1))
   if(new_i % (maxX - minX + 1) == 0){
    coordinate(1) = (maxY - (Math.floor (new_i / (maxX - minX + 1)) - 1)).toInt
    coordinate(0) = (minX + new_i - (Math.floor (new_i / (maxX - minX + 1))-1) * (maxX - minX + 1) - 1 ).toInt
    return coordinate
   }else{
    coordinate(1) = (maxY - Math.floor (new_i / (maxX - minX + 1))).toInt
    coordinate(0) = (minX + new_i - Math.floor (new_i / (maxX - minX + 1)) * (maxX - minX + 1) - 1 ).toInt
    return coordinate
    }
   
  }

  
 

  // YOU NEED TO CHANGE THIS PART
}
