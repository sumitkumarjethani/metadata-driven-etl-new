package utils

import java.io.File
import java.nio.file.{Files, Paths}

object Utils {
  def pathExists(path: String): Boolean = {
    /**
     * Comprueba si un directorio o archivo existe en la ruta especificada.
     *
     * @param path la ruta del directorio o archivo a comprobar
     * @return true si el directorio o archivo existe, false en caso contrario
     */
    Files.exists(Paths.get(path))
  }

  def isDirectory(path: String): Boolean = {
    /**
     * Comprueba si la ruta es un directorio.
     *
     * @param path Ruta a comprobar.
     * @return True si la ruta es un directorio, False en caso contrario.
     */
    val file = new File(path)
    file.isDirectory
  }

  def getDirectoryFileNames(path: String): Array[String] = {
    /**
     * Devuelve un array con los nombres de los archivos en un directorio dado.
     *
     * @param path la ruta del directorio.
     * @return un array con los nombres de los archivos.
     */
    new File(path).listFiles.filter(_.isFile).map(_.getName)
  }

  def getFileNameWithoutExtensionFromPath(path: String): String = {
    /**
     * Devuelve el nombre de archivo sin extensión de una ruta dada.
     *
     * @param path La ruta del archivo.
     * @return El nombre del archivo sin extensión.
     */
    val fileName = new File(path).getName
    fileName.substring(0, fileName.lastIndexOf('.'))
  }

}
