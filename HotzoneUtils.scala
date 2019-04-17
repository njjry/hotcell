package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val double_rectangle = queryRectangle.split(",").map(_.toDouble)
    val double_point = pointString.split(",").map(_.toDouble)
    if ((double_point(0) >= double_rectangle(0)) && (double_point(0) <= double_rectangle(2))){
      if ((double_point(1) >= double_rectangle(1)) && (double_point(1) <= double_rectangle(3))){
         //println(queryRectangle + ":" + pointString + " true")
         return true 
      }
    }
    //println(queryRectangle + ":" + pointString + " false")
    return false
  }

  // YOU NEED TO CHANGE THIS PART

}
