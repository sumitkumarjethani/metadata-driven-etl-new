package etl
import metadata.components.Source
import metadata.components.types.FormatType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.Map
import utils.Utils

class Extractor (spark: SparkSession) {

  def load(sources: List[Source]): Map[String, Map[String, DataFrame]] = {
    if (sources.length == 0) throw new Exception("No existen origenes definidos en el fichero de metadatos")

    val sourcesMap = Map[String, Map[String, DataFrame]]()

    sources.map { source =>
      if (!Utils.pathExists(source.path)) {
        throw new Exception(s"La ruta ${source.path} del origen: ${source.name} no existe")
      }

      if (!FormatType.values.toList.contains(source.format)) {
        throw new IllegalArgumentException(
          s"Formato del fichero ${source.format} no soportado para el origen ${source.name}"
        )
      }

      val sourceType = if (Utils.isDirectory(source.path)) "directory" else "file"

      val sourceMap: Map[String, DataFrame] = source.format match {
        case FormatType.JSON =>
          sourceType match {
            case "file" => Map(source.path -> spark.read.json(source.path))
            case "directory" => {
              val directoryMap = Map[String, DataFrame]()
              for (file <- Utils.getDirectoryFileNames(source.path)) {
                val df = spark.read.json(source.path + '/' + file)
                directoryMap += ((source.path + '/' + file) -> df)
              }
              directoryMap
            }
            case _ => throw new Exception("Algo salió mal")
          }
      }
      sourcesMap += (source.name -> sourceMap)
    }
    sourcesMap
  }
}
