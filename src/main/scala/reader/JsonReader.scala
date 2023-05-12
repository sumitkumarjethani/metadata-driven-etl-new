package reader

class JsonReader(path: String) extends Reader(path) {
  import play.api.libs.json._

  def readJson(): JsValue = {
    /**
     * Lee el contenido del fichero y lo parsea como un objeto Json.
     *
     * @return Objeto Json parseado.
     * @throws Exception Si ocurre un error al parsear el fichero leído.
     */
    val content = read()
    try {
      Json.parse(content)
    } catch {
      case _: Exception => throw new Exception("Error al parsear el fichero leido")
    }
  }
}
