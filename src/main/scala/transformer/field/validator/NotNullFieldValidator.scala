package transformer.field.validator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class NotNullFieldValidator extends FieldValidator {
  override def validate(fieldName: String, df: DataFrame): DataFrame = {
    /**
     * Función que valida si un campo en un DataFrame no es nulo
     *
     * @param fieldName nombre del campo a validar
     * @param df        DataFrame donde se encuentra el campo a validar
     * @return DataFrame con una nueva columna "arraycoderrorbyfield" que contiene un array de códigos de error
     *         correspondientes a los errores encontrados en la validación
     * @throws Exception si el campo no existe en el DataFrame
     */
    if (!df.columns.contains(fieldName)) {
      throw new Exception(s"Campo ${fieldName} no existe")
    }

    // Comprobar si la columna "arraycoderrorbyfield" existe en el dataframe
    val updatedDf = if (df.columns.contains("arraycoderrorbyfield")) {
      df
    } else {
      df.withColumn("arraycoderrorbyfield", array())
    }

    // Comprobar por fila si la columna es nula y añadir error en caso afirmativo
    val newDf = updatedDf.withColumn("arraycoderrorbyfield",
      when(col(fieldName).isNull, array_union(
        col("arraycoderrorbyfield"),
        array(lit(s"${fieldName}: es nula"))
      )).otherwise(col("arraycoderrorbyfield"))
    )
    newDf
  }
}
