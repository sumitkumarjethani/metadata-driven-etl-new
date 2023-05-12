package writer

import metadata.components.types.SaveModeType
import org.apache.spark.sql.DataFrame
import utils.Utils._

class JsonWriter extends Writer {
  override def write(df: DataFrame, path: String, fileName: String, saveModeType: SaveModeType.SaveModeType): Unit = {
    if (!SaveModeType.values.exists(_ == saveModeType)) throw new Exception("Modo de guardado no soportado")
    if(!pathExists(path)) throw new Exception(s"Ruta de guardado: ${path} no existe")
    if(!isDirectory(path)) throw new Exception(s"Ruta de guardado: ${path} no es un directorio")
    //df.write.mode(saveModeType.toString.toLowerCase()).json("file:///" + path + ".json")
    println(df.show())
  }
}
