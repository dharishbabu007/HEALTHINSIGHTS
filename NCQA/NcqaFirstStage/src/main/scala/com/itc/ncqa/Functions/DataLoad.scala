package com.itc.ncqa.Functions



import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._
import org.apache.spark.sql.Row
import java.sql.Date



object DataLoad {


  def convertRowtoRowRdd(data: String, validationDetails: ArrayBuffer[String]) = {

    var rowItem: ArrayBuffer[Any] = new ArrayBuffer[Any]
    val dataLength = validationDetails.map(foreach => foreach.split(",")(2).toInt).toArray.sum
    /* println("arraylength:"+dataLength)
     println("datalength:"+data.length())*/
    if (data != null) {

      //if (data.length() != dataLength)
      if (data.length() != dataLength){
        rowItem.+=(data).+=("Error_datalengthdifference")
      } else {
        breakable {
          var strt = 0
          var endIndex =0
          println("at 0"+ data.charAt(0))
          for (i <- 0 to (validationDetails.length - 1)) {
            try {
              val length = validationDetails(i).split(",")(2).toInt
              endIndex = strt + (length)
              if(endIndex > data.length()) endIndex = data.length()
              //println("Endindex:"+endIndex)
              val strData = data.substring(strt, endIndex)
              val typeVal = validationDetails(i).split(",")(3) match {

                case "INT"            => strData.toInt
                case "STRING"       => strData.trim()
                case "DATE" => {
                  val dateString = strData.substring(0, 4)+"-"+strData.substring(4, 6) + "-" + strData.substring(6, 8)
                  Date.valueOf(dateString)
                }
                case "BOOLEAN"        => { if (strData.equals("Y")) true else false }
              }
              println("value is :"+typeVal)
              rowItem.+=(typeVal)
            } catch {
              case e: NullPointerException => {
                rowItem.+=:(data).+=("Error_nulldata")
                break()
              }
              case e: ArrayIndexOutOfBoundsException => {
                rowItem.+=:(data).+=("Error_3")
                break()
              }
              case e: ClassCastException => {
                rowItem.+=:(data).+=("Error_datatypemismatchfromschema")
                break()
              }
              case e: NumberFormatException => {
                rowItem.+=:(data).+=("Error_numberconversionerror")
                break()
              }
            }/*catch block ends*/

            strt = endIndex
          }/*for loop ends*/
        }/*breakable block ends*/
      } /*else ends */
    }/*data null check ends*/
    Row.fromSeq(rowItem)
  }

}
