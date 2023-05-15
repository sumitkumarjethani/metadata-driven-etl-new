package etl

import metadata.components.types.{FormatType, SaveStatusType}
import metadata.components.Sink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import writer.{CsvWriter, JsonWriter}
import utils.Utils

import scala.collection.mutable.{Map => MutableMap}

class Loader {
  def load(sinks: List[Sink], sourcesMap: MutableMap[String, MutableMap[String, DataFrame]]): Unit = {
    /**
     * Carga los datos a los destinos especificados en la lista de sinks.
     *
     * @param sinks      lista de sinks que especifican el destino donde cargar los datos.
     * @param sourcesMap mapa que contiene los datos de entrada identificados por un nombre.
     * @throws Exception si el input especificado en un sink no existe en el mapa de fuentes.
     *                   si se produce un error al guardar los datos en el destino.
     */
    for(sink <- sinks) {
      if(!sourcesMap.contains(sink.input))
        throw new Exception(s"Input: ${sink.input} para el destino: ${sink.name} no existe")

      sink.status match {
        case SaveStatusType.OK => {
          saveOkDfs(sink, sourcesMap(sink.input))
        }
        case SaveStatusType.KO => {
          saveKoDfs(sink, sourcesMap(sink.input))
        }
        case _ => throw new Exception("Error al guardar los datos en el destinto")
      }
    }
  }

  private def saveOkDfs(sink: Sink, dfsMap: MutableMap[String, DataFrame]): Unit = {
    /**
     * Guarda los DataFrames que han sido procesados sin errores en el Sink especificado.
     *
     * @param sink   El Sink donde se guardará el DataFrame.
     * @param dfsMap El MutableMap que contiene los DataFrames a guardar.
     */
    val writer = sink.format match {
      case FormatType.JSON => new JsonWriter()
      case FormatType.CSV => new CsvWriter()
      case _ => throw new Exception(s"Formato de almacenado: ${sink.format} no soportado")
    }

    for((path, df) <- dfsMap) {
      var okDf = df
      if (okDf.columns.contains("arraycoderrorbyfield"))
        okDf = df.filter(size(col("arraycoderrorbyfield")) === 0).drop("arraycoderrorbyfield")
      for(savePath <- sink.paths) {
        writer.write(
          okDf,
          savePath, utils.Utils.getFileNameWithoutExtensionFromPath(path),
          sink.saveMode
        )
      }
    }
  }

  private def saveKoDfs(sink: Sink, dfsMap: MutableMap[String, DataFrame]): Unit = {
    /**
     * Guarda los DataFrames que han sido procesados con errores en el Sink especificado.
     *
     * @param sink   El Sink donde se guardará el DataFrame.
     * @param dfsMap El MutableMap que contiene los DataFrames a guardar.
     */
    val writer = sink.format match {
      case FormatType.JSON => new JsonWriter()
      case FormatType.CSV => new CsvWriter()
      case _ => throw new Exception(s"Formato de almacenado: ${sink.format} no soportado")
    }

    for ((path, df) <- dfsMap) {
      var koDf = df
      if (koDf.columns.contains("arraycoderrorbyfield")) koDf = df.filter(size(col("arraycoderrorbyfield")) > 0)
      for (savePath <- sink.paths) {
        writer.write(
          koDf,
          savePath, Utils.getFileNameWithoutExtensionFromPath(path),
          sink.saveMode
        )
      }
    }
  }
}
