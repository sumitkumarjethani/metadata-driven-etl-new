package writer

import metadata.components.types.SaveModeType
import org.apache.spark.sql.DataFrame
import utils.Utils._

class CsvWriter extends Writer {
  override def write(df: DataFrame, path: String, fileName: String, saveModeType: SaveModeType.SaveModeType): Unit = {
    if (!SaveModeType.values.exists(_ == saveModeType)) throw new Exception("Modo de almacenado no soportado")
    if(!pathExists(path)) throw new Exception(s"Ruta de guardado: ${path} no existe")
    if(!isDirectory(path)) throw new Exception(s"Ruta de guardado: ${path} no es un directorio")
    if(!df.isEmpty) {
      df.coalesce(1)
        .write.option("header", "true")
        .mode(saveModeType.toString.toLowerCase())
        .csv("file:///" + path + "/" + fileName)
    }
  }
}