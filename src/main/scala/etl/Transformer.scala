package etl
import metadata.components.Transformation
import metadata.components.types.TransformationType
import org.apache.spark.sql.DataFrame
import transformer.field.additor.{FieldAdditorConcat, FieldAdditorCurrentTimestamp, FieldAdditionType}
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.immutable.{Map => ImmutableMap}

class Transformer () {
  def transform(transformations: List[Transformation],
                sourcesMap: MutableMap[String, MutableMap[String, DataFrame]]
  ): MutableMap[String, MutableMap[String, DataFrame]] = {
    /**
     * Aplica transformaciones a un conjunto de DataFrames.
     *
     * @param transformations lista de transformaciones a aplicar.
     * @param sourcesMap      mapa de DataFrames sobre los que aplicar las transformaciones.
     * @return un mapa de DataFrames con las transformaciones aplicadas.
     * @throws Exception si ocurre un error al aplicar las transformaciones.
     */

    var newSourcesMap = sourcesMap.clone()

    val notValidateTransformations: List[Transformation] =
      transformations.filterNot(_.`type` eq TransformationType.VALIDATE_FIELDS)

    if (notValidateTransformations.length == 0) return sourcesMap

    notValidateTransformations.foreach { transformation =>
      transformation.`type` match {
        case TransformationType.ADD_FIELDS => {
          newSourcesMap = addFieldsTransformation(transformation, sourcesMap)
        }
        case _ => throw new Exception(s"Error tipo de transformación ${transformation.`type`} no soportada")
      }
    }
    newSourcesMap
  }

  private def addFieldsTransformation(transformation: Transformation,
                              sourcesMap: MutableMap[String, MutableMap[String, DataFrame]]
  ): MutableMap[String, MutableMap[String, DataFrame]] = {
    /**
     * Función privada que aplica la transformación "addFields" a los dataframes de una fuente de datos.
     * La transformación "addFields" añade campos a los dataframes.
     *
     * @param transformation objeto de tipo Transformation que representa la transformación "addFields".
     * @param sourcesMap     mapa de fuentes de datos donde se encuentran los dataframes a los que se les aplica la transformación.
     * @return mapa de fuentes de datos con los dataframes transformados.
     * @throws Exception si no se especifica alguno de los parámetros necesarios para la transformación o si se intenta
     *                   aplicar una transformación no soportada.
     */

    if (!transformation.params.contains("input"))
      throw new Exception(s"Parametro input no especificado en la transformación: ${transformation.name}")
    if (!transformation.params.contains("addFields"))
      throw new Exception(s"Parametro addFields no especificado en la transformación: ${transformation.name}")
    if (!sourcesMap.contains(transformation.params("input").asInstanceOf[String]))
      throw new Exception(s"Input: ${transformation.params("input").asInstanceOf[String]} de la transformación: ${transformation.name} no existe")

    val input = transformation.params("input").asInstanceOf[String]
    val addFieldsMapList = transformation.params("addFields").asInstanceOf[List[ImmutableMap[String, Any]]]
    val internalMap = sourcesMap(input)

    for ((path, df) <- internalMap) {
      var updatedDf = df
      for (addFieldMap <- addFieldsMapList) {
        if(!addFieldMap.contains("name"))
          throw new Exception("Nombre de campo no especificado en alguna transformación")
        if(!addFieldMap.contains("function"))
          throw new Exception("Nombre de función no especificado en alguna transformación")

        val fieldName = addFieldMap("name").asInstanceOf[String]
        val functionName = addFieldMap("function").asInstanceOf[String]

        FieldAdditionType.fromString(functionName) match {
          case FieldAdditionType.CURRENT_TIMESTAMP => {
            updatedDf = new FieldAdditorCurrentTimestamp(fieldName).add(updatedDf)
          }
          case FieldAdditionType.CONCAT => {
            if (!addFieldMap.contains("fields") || addFieldMap("fields").asInstanceOf[List[String]].length == 0)
              throw new Exception("Nombre de campos no especificados para la concatenación")
            if (!addFieldMap.contains("separator"))
              throw new Exception("Separador de concatenación no especificado")

            val inputFields = addFieldMap("fields").asInstanceOf[List[String]]
            val separator = addFieldMap("separator").asInstanceOf[String]
            updatedDf = new FieldAdditorConcat(fieldName, separator, inputFields).add(updatedDf)
          }
          case _ => throw new Exception(s"Error tipo de transformación: ${FieldAdditionType.fromString(functionName)} no soportada")
        }
      }
      internalMap.update(path, updatedDf)
    }
    sourcesMap.update(input, internalMap)
    sourcesMap
  }
}
