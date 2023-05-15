package reader

import utils.Utils
import java.io.File
import scala.io.Source
import play.api.libs.json._
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonReader extends Reader {

  def read(path: String): JsValue = {
    /**
     * Lee el contenido de un archivo en formato JSON y devuelve el resultado como un objeto JsValue.
     *
     * @param path Ruta del archivo JSON a leer.
     * @return Objeto JsValue que representa el contenido del archivo JSON.
     * @throws Exception si la ruta de lectura no existe o si ocurre un error al parsear el archivo.
     */
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
    /**
     * Lee el contenido de un archivo en formato JSON y devuelve el resultado como un DataFrame de Spark.
     *
     * @param path  Ruta del archivo JSON a leer.
     * @param spark Sesión de Spark utilizada para la lectura del archivo.
     * @return DataFrame de Spark que contiene el contenido del archivo JSON.
     * @throws Exception si la ruta de lectura no existe o si ocurre un error al parsear el archivo.
     */
    if (!Utils.pathExists(path)) throw new Exception(s"Ruta de lectura: ${path} no existe")
    try {
      spark.read.option("inferSchema", "true").json(path)
    } catch {
      case _: Exception => throw new Exception("Error al parsear el fichero leido")
    }
  }
}
