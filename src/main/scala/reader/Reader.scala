package reader
import scala.io.Source
import utils.Utils
import java.io.File

class Reader(path: String) {
  require(Utils.pathExists(path), "La ruta de los metadatos no existe")
  private val source: Source = Source.fromFile(new File(path))

  def read(): String = {
    /**
     * Lee el contenido del archivo y lo devuelve como una cadena.
     *
     * @return Contenido del archivo como una cadena.
     * @throws Exception si hay algún error al leer el archivo.
     */
    try {
      source.getLines().mkString("\n")
    } catch {
      case _: Exception => throw new Exception("Error al leer el fichero")
    } finally {
      source.close()
    }
  }
}
