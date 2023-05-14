package writer

import metadata.components.types.SaveModeType
import org.apache.spark.sql.DataFrame
import utils.Utils._

class JsonWriter extends Writer {
  override def write(df: DataFrame, path: String, fileName: String, saveModeType: SaveModeType.SaveModeType): Unit = {
    /**
     * Escribe un dataframe en un directorio especificado.
     *
     * @param df           DataFrame a guardar.
     * @param path         Directorio donde guardar el DataFrame.
     * @param fileName     Nombre del archivo donde guardar el DataFrame.
     * @param saveModeType Modo de guardado para el DataFrame.
     */
    if (!SaveModeType.values.exists(_ == saveModeType)) throw new Exception("Modo de almacenado no soportado")
    if(!pathExists(path)) throw new Exception(s"Ruta de guardado: ${path} no existe")
    if(!isDirectory(path)) throw new Exception(s"Ruta de guardado: ${path} no es un directorio")
    df.coalesce(1)
      .write.mode(saveModeType.toString.toLowerCase())
      .json("file:///" + path + "/" + fileName)
  }
}
