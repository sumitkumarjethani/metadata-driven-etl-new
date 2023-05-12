package etl

import metadata.components.types.{FormatType, SaveStatusType}
import metadata.components.Sink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import writer.JsonWriter

import scala.collection.mutable.{Map => MutableMap}

class Loader {
  def load(sinks: List[Sink], sourcesMap: MutableMap[String, MutableMap[String, DataFrame]]): Unit = {
    for(sink <- sinks) {
      if(!sourcesMap.contains(sink.input)) throw new Exception("Error al guardar los datos")
      sink.status match {
        case SaveStatusType.OK => {
          saveOkDfs(sink, sourcesMap(sink.input))
        }
        case SaveStatusType.KO => {
          saveKoDfs(sink, sourcesMap(sink.input))
        }
        case _ => throw new Exception("Error al guardar los datos")
      }
    }
  }

  def saveOkDfs(sink: Sink, dfsMap: MutableMap[String, DataFrame]): Unit = {
    sink.format match {
      case FormatType.JSON => {
        val jsonWriter = new JsonWriter()
        for((_, df) <- dfsMap) {
          var okDf = df
          if (okDf.columns.contains("arraycoderrorbyfield")) okDf = df.filter(size(col("arraycoderrorbyfield")) === 0)
          for(savePath <- sink.paths) {
            jsonWriter.write(
              okDf,
              savePath, sink.name,
              sink.saveMode
            )
          }
        }
      }
      case _ => throw new Exception("Error al guardar los datos")
    }
  }

  def saveKoDfs(sink: Sink, dfsMap: MutableMap[String, DataFrame]): Unit = {
    sink.format match {
      case FormatType.JSON => {
        val jsonWriter = new JsonWriter()
        for ((_, df) <- dfsMap) {
          var koDf = df
          if (koDf.columns.contains("arraycoderrorbyfield")) koDf = df.filter(size(col("arraycoderrorbyfield")) > 0)
          for (savePath <- sink.paths) {
            jsonWriter.write(
              koDf,
              savePath, sink.name,
              sink.saveMode
            )
          }
        }
      }
      case _ => throw new Exception("Error al guardar los datos")
    }
  }
}
