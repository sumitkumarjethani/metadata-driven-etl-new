package etl
import metadata.components.Source
import metadata.components.types.FormatType
import org.apache.spark.sql.{DataFrame, SparkSession}
import reader.JsonReader

import scala.collection.mutable.{Map => MutableMap}
import utils.Utils

class Extractor (spark: SparkSession) {

  def load(sources: List[Source]): MutableMap[String, MutableMap[String, DataFrame]] = {
    /**
     * Carga los datos de las fuentes proporcionadas en una estructura de mapa mutable.
     *
     * @param sources Lista de fuentes de datos.
     * @return Mapa mutable que contiene los datos cargados, donde la clave externa es el nombre de la fuente
     *         y la clave interna es la ruta del archivo o directorio, y el valor es el DataFrame resultante.
     * @throws Exception si no se han definido fuentes para cargar los datos.
     */

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
        case _ => throw new Exception(s"Formato de lectura: ${source.format} no soportado")
      }
      sourcesMap += (source.name -> sourceMap)
    }
    sourcesMap
  }
}
