package etl

import field.validator.{FieldValidation, FieldValidationType, NotNullFieldValidator}
import metadata.components.Transformation
import metadata.components.types.TransformationType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.immutable.{Map => ImmutableMap}

class Validator () {

  def validate(
      transformations: List[Transformation], sourcesMap: MutableMap[String, MutableMap[String, DataFrame]]
  ): MutableMap[String, MutableMap[String, DataFrame]] = {

    val validateFieldsTransformations : List[Transformation] =
      transformations.filter(_.`type` eq TransformationType.VALIDATE_FIELDS)

    if (validateFieldsTransformations.length == 0) return sourcesMap

    val paramsList = validateFieldsTransformations.map {transformation =>
      if (
        !transformation.params.contains("input") || !transformation.params.contains("validations") ||
        !sourcesMap.contains(transformation.params("input").asInstanceOf[String])
      ) {
        throw new Exception("Error en la validación")
      }
      transformation.params
    }

    for (param <- paramsList) {
      val input = param("input").asInstanceOf[String]
      val validationMapList = param("validations").asInstanceOf[List[ImmutableMap[String, Any]]]
      val fieldValidationList = validationMapList.map(m =>
        try {
          FieldValidation(
            m("field").toString,
            m("validations").asInstanceOf[List[String]].map(FieldValidationType.fromString)
          )
        } catch {
          case _: Exception => throw new Exception("Error en la validación")
        }
      )
      val fieldValidationsFlat = fieldValidationList.flatMap(validation => validation.validations.map((validation.field, _)))
      val internalMap = sourcesMap(input)

      for((path, df) <- internalMap) {
        var updatedDf = df
        for ((fieldName, fieldValidationType) <- fieldValidationsFlat) {
          fieldValidationType match {
            case FieldValidationType.NOT_NULL => {
              updatedDf = new NotNullFieldValidator().validate(fieldName, updatedDf)
            }
            case _ => throw new Exception("Error en la validación")
          }
        }
        internalMap.update(path, updatedDf)
      }
      sourcesMap.update(input, internalMap)
    }
    sourcesMap
  }
}
