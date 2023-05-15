package metadata.parser

import metadata.components._
import metadata.components.types.{FormatType, SaveModeType, SaveStatusType, TransformationType}
import org.json4s._
import org.json4s.jackson.JsonMethods
import play.api.libs.json.{JsDefined, JsUndefined, JsValue}
import scala.util.Try

class JsonMetadataParser extends MetadataParser {
  private def parseSource(json: JsValue): Option[Source] = {
    /**
     * Convierte un objeto JSON en una instancia de la clase Source.
     *
     * @param json objeto JSON a parsear.
     * @return una instancia de Source si el parseo es correcto, None si no se puede parsear.
     * @throws Exception si hay un error al parsear el objeto JSON.
     */
    val name = try {
      (json \ "name").as[String]
    } catch {
      case _: Exception => throw new Exception("Nombre de source no especificado en la lectura")
    }

    val path = try {
      (json \ "path").as[String]
    } catch {
      case _: Exception => throw new Exception("Nombre de la ruta no especificado en la lectura")
    }

    val format = Try(FormatType.withName((json \ "format").as[String])).getOrElse(FormatType.JSON)
    Some(Source(name, path, format))
  }

  private def parseSink(json: JsValue): Option[Sink] = {
    /**
     * Convierte un objeto JSON en una instancia de la clase Sink.
     *
     * @param json el objeto JSON que se va a convertir en un Sink.
     * @return Una opción Some(Sink) con la instancia Sink creada si se pudieron extraer los valores necesarios del JSON, None si no se pudo crear.
     * @throws Exception Si no se pudo obtener el nombre del destino o del origen a partir del JSON.
     */
    val name = try {
      (json \ "name").as[String]
    } catch {
      case _: Exception => throw new Exception("Nombre del fichero destino no especificado para la escritura")
    }

    val input = try {
      (json \ "input").as[String]
    } catch {
      case _: Exception => throw new Exception("Nombre de source no especificado para la escritura")
    }

    val paths = Try((json \ "paths").as[List[String]]).getOrElse(Nil)
    val saveMode = Try(SaveModeType.withName((json \ "saveMode").as[String])).getOrElse(SaveModeType.APPEND)
    val status = Try(SaveStatusType.withName((json \ "status").as[String])).getOrElse(SaveStatusType.OK)
    val format = Try(FormatType.withName((json \ "format").as[String])).getOrElse(FormatType.JSON)

    Some(Sink(input, status, name, paths, format, saveMode))
  }

  private def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    /**
     * Parsea una string JSON a un Map[String, Any] para la representación de los parámetros de la fase de transformación.
     *
     * @param jsonStr La cadena JSON a parsear.
     * @return Un Map[String, Any] que representa los parámetros del la fase de transformación.
     */
    implicit val formats = org.json4s.DefaultFormats
    JsonMethods.parse(jsonStr).extract[Map[String, Any]]
  }

  private def parseTransformation(json: JsValue): Option[Transformation] = {
    /**
     * Convierte un objeto JSON en una instancia de la clase Transformation
     *
     * @param json Objeto JSON a ser parseado
     * @return objeto de la clase Transformation
     */
    val name = Try((json \ "name").as[String]).getOrElse("")
    val ttype = Try(TransformationType.withName((json \ "type").as[String])).getOrElse(TransformationType.UNKNOWN)
    val params = jsonStrToMap((json \ "params") match {
      case JsDefined(value) => value.toString
      case _: JsUndefined => ""
    })
    if (name.nonEmpty && ttype != TransformationType.UNKNOWN) Some(Transformation(name, ttype, params)) else None
  }

  override def parse(metadata: Any): DataFlow = {
    /**
     * Parsea los metadatos recibidos en un objeto DataFlow
     *
     * @param metadata Los metadatos en formato JsValue
     * @return Un objeto DataFlow
     * @throws IllegalArgumentException si los metadatos no son un objeto JsValue
     * @throws Exception                si los metadatos no pueden ser parseados a DataFlow
     */
    if (!metadata.isInstanceOf[JsValue]) throw new IllegalArgumentException("Los metadatos deben ser de tipo JSValue")

    val jsValue = metadata.asInstanceOf[JsValue]
    val dataflows = (jsValue \ "dataflows").as[List[JsValue]]

    if (dataflows.nonEmpty) {
      val sources = dataflows.flatMap(df => (df \ "sources").as[List[JsValue]].flatMap(parseSource))
      val transformations = dataflows.flatMap(df => (df \ "transformations").as[List[JsValue]].flatMap(parseTransformation))
      val sinks = dataflows.flatMap(df => (df \ "sinks").as[List[JsValue]].flatMap(parseSink))

      DataFlow(
          Try((dataflows.head \ "name").as[String]).getOrElse(""),
          sources, transformations, sinks
      )
    } else {
      throw new Exception("Error al parsear los metadatos a DataFlow")
    }
  }
}
