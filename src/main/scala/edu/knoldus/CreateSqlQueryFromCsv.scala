package edu.knoldus
import java.io.{File, FileWriter}
import scala.io.Source

object CreateSqlQueryFromCsv extends App {
  val filePath = "src/main/resources/countries.csv"

  def printQuery(
                  pId:Int,
                  countryCode: String,
                  latitude: String,
                  longitude: String,
                  countryName: String,
                  pw: FileWriter
                ): Int = {
    val data = s"insert into remote_areas (id, countryCode, latitude, longitude, countryName) " +
      s"values ('$pId','$countryCode', '$latitude', '$longitude', '$countryName') " +
      s"on conflict (countryCode, latitude, longitude, countryName) do nothing;"
    pw.write(data +"\n")
    pId + 1
  }

  def readAddress(file: String): Unit = {
    val step = 50
    var pId = 1;
    Source.fromFile(file).getLines()
      .sliding(step, step)
      .zipWithIndex
      .foreach { case (seq, i) =>
        val pw: FileWriter = new FileWriter(new File("countries" + i+".sql"), false)
        seq.foreach { ff =>
          val f: Array[String] = ff.split(",")
          val countryCode = f.apply(0)
          val latitude = f.apply(1)
          val longitude = f.apply(2)
          val countryName = f.apply(3)

            pId = printQuery(pId, countryCode, latitude, longitude, countryName, pw)
        }
        pw.close()
      }
  }

  readAddress(filePath)

}

