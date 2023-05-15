package etl

import metadata.components.Transformation
import metadata.components.types.TransformationType
import org.apache.spark.sql.DataFrame
import transformer.field.validator.{FieldValidation, FieldValidationType, IsIntFieldValidator, NotNullFieldValidator}

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.immutable.{Map => ImmutableMap}

class Validator () {

  def validate(
      transformations: List[Transformation], sourcesMap: MutableMap[String, MutableMap[String, DataFrame]]
  ): MutableMap[String, MutableMap[String, DataFrame]] = {
    /**
     * Valida los campos de los DataFrames en el mapa de fuentes con respecto a las especificaciones de validación
     * proporcionadas en la lista de transformaciones. Si no hay transformaciones de validación de campos, se devuelve
     * el mapa de fuentes original sin modificaciones. La función devuelve el mapa de fuentes actualizado con los
     * DataFrames validados.
     *
     * @param transformations La lista de transformaciones a aplicar, donde solo se aplicarán las transformaciones
     *                        de validación de campos.
     * @param sourcesMap      El mapa de fuentes que contiene los DataFrames a validar.
     * @return El mapa de fuentes actualizado con los DataFrames validados.
     * @throws Exception Si una transformación de validación de campos no especifica los parámetros necesarios,
     *                   o si el input especificado no existe en el mapa de fuentes.
     */

    val validateFieldsTransformations : List[Transformation] =
      transformations.filter(_.`type` eq TransformationType.VALIDATE_FIELDS)

    if (validateFieldsTransformations.length == 0) return sourcesMap

    val paramsList = validateFieldsTransformations.map {transformation =>
      if (!transformation.params.contains("input"))
        throw new Exception(s"Parametro input no especificado en la transformación: ${transformation.name}")
      if (!transformation.params.contains("validations"))
        throw new Exception(s"Parametro validations no especificado en la transformación: ${transformation.name}")
      if (!sourcesMap.contains(transformation.params("input").asInstanceOf[String]))
        throw new Exception(s"Input: ${transformation.params("input").asInstanceOf[String]} de la transformación: ${transformation.name} no existe")

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
          case _: Exception => throw new Exception("Error al parsear la validación")
        }
      )
      val fieldValidationsFlat = fieldValidationList.flatMap(
        validation => validation.validations.map((validation.field, _)))

      val internalMap = sourcesMap(input)

      for((path, df) <- internalMap) {
        var updatedDf = df
        for ((fieldName, fieldValidationType) <- fieldValidationsFlat) {
          fieldValidationType match {
            case FieldValidationType.NOT_NULL => {
              updatedDf = new NotNullFieldValidator().validate(fieldName, updatedDf)
            }
            case FieldValidationType.IS_INT => {
              updatedDf = new IsIntFieldValidator().validate(fieldName, updatedDf)
            }
            case _ => throw new Exception(s"Validación: ${fieldValidationType} no soportada")
          }
        }
        internalMap.update(path, updatedDf)
      }
      sourcesMap.update(input, internalMap)
    }
    sourcesMap
  }
}
