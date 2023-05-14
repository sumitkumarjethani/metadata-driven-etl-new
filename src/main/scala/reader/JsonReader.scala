package reader

import utils.Utils
import java.io.File
import scala.io.Source
import play.api.libs.json._
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonReader extends Reader {

  def read(path: String): JsValue = {
    if(!Utils.pathExists(path)) throw new Exception(s"Ruta de lectura: ${path} no existe")
    val source = Source.fromFile(new File(path))

    try {
      Json.parse(source.getLines().mkString("\n"))
    } catch {
      case _: Exception => throw new Exception("Error al parsear el fichero leido")
    } finally {
      source.close()
    }
  }

  def read(path: String, spark: SparkSession): DataFrame = {
    if (!Utils.pathExists(path)) throw new Exception(s"Ruta de lectura: ${path} no existe")
    try {
      spark.read.option("inferSchema", "true").json(path)
    } catch {
      case _: Exception => throw new Exception("Error al parsear el fichero leido")
    }
  }
}
