package etl
import metadata.components.Source
import metadata.components.types.FormatType
import org.apache.spark.sql.{DataFrame, SparkSession}
import reader.JsonReader

import scala.collection.mutable.{Map => MutableMap}
import utils.Utils

class Extractor (spark: SparkSession) {

  def load(sources: List[Source]): MutableMap[String, MutableMap[String, DataFrame]] = {
    if (sources.length == 0) throw new Exception("No sources definidos para iniciar la extracción")

    val sourcesMap = MutableMap[String, MutableMap[String, DataFrame]]()

    sources.map { source =>
      if (!Utils.pathExists(source.path)) {
        throw new Exception(s"La ruta ${source.path} del source: ${source.name} no existe")
      }

      if (!FormatType.values.toList.contains(source.format)) {
        throw new IllegalArgumentException(
          s"Formato del fichero ${source.format} no soportado para el source ${source.name}"
        )
      }

      val sourceType = if (Utils.isDirectory(source.path)) "directory" else "file"

      val sourceMap: MutableMap[String, DataFrame] = source.format match {
        case FormatType.JSON =>
          val jsonReader = new JsonReader()
          sourceType match {
            case "file" => MutableMap(source.path -> jsonReader.read(source.path, spark))
            case "directory" => {
              val directoryMap = MutableMap[String, DataFrame]()
              for (file <- Utils.getDirectoryFileNames(source.path)) {
                val df = jsonReader.read(source.path + '/' + file, spark)
                directoryMap += ((source.path + '/' + file) -> df)
              }
              directoryMap
            }
          }
      }
      sourcesMap += (source.name -> sourceMap)
    }
    sourcesMap
  }
}
