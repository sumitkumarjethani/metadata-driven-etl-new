package etl
import metadata.components.Transformation
import metadata.components.types.TransformationType
import org.apache.spark.sql.DataFrame
import transformer.field.additor.{FieldAdditionCurrentTimestamp, FieldAdditionType}

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.immutable.{Map => ImmutableMap}

class Transformer () {
  def transform(transformations: List[Transformation],
                sourcesMap: MutableMap[String, MutableMap[String, DataFrame]]
  ): MutableMap[String, MutableMap[String, DataFrame]] = {

    var newSourcesMap = sourcesMap.clone()

    val notValidateTransformations: List[Transformation] =
      transformations.filterNot(_.`type` eq TransformationType.VALIDATE_FIELDS)

    if (notValidateTransformations.length == 0) return sourcesMap

    notValidateTransformations.foreach { transformation =>
      transformation.`type` match {
        case TransformationType.ADD_FIELDS => {
          newSourcesMap = addFieldsTransformation(transformation, sourcesMap)
        }
        case _ => throw new Exception("Tipo de transformación desconocida")
      }
    }
    newSourcesMap
  }

  def addFieldsTransformation(transformation: Transformation,
                              sourcesMap: MutableMap[String, MutableMap[String, DataFrame]]
  ): MutableMap[String, MutableMap[String, DataFrame]] = {

    if(!transformation.params.contains("input") || !transformation.params.contains("addFields")
          || !sourcesMap.contains(transformation.params("input").asInstanceOf[String])
    ) throw new Exception("Error en la transformacion")

    val params = transformation.params
    val input = params("input").asInstanceOf[String]
    val addFieldsMapList = params("addFields").asInstanceOf[List[ImmutableMap[String, String]]]
    val internalMap = sourcesMap(input)

    for ((path, df) <- internalMap) {
      var updatedDf = df
      for (addFieldMap <- addFieldsMapList) {
        if(!addFieldMap.contains("name") || !addFieldMap.contains("function")) throw new Exception("Error en la transformación")
        val fieldName = addFieldMap("name")
        val functionName = addFieldMap("function")
        FieldAdditionType.fromString(functionName) match {
          case FieldAdditionType.CURRENT_TIMESTAMP => {
            updatedDf = new FieldAdditionCurrentTimestamp(fieldName).add(updatedDf)
          }
          case _ => throw new Exception("Error en la transformación")
        }
      }
      internalMap.update(path, updatedDf)
    }
    sourcesMap.update(input, internalMap)
    sourcesMap
  }
}
