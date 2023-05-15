package transformer.validator.field

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class IsIntFieldValidator extends FieldValidator {
  override def validate(fieldName: String, df: DataFrame): DataFrame = {
    /**
     * Valida si una columna específica del DataFrame es de tipo Int y agrega información
     * de error en caso necesario.
     *
     * @param fieldName Nombre de la columna a validar.
     * @param df        DataFrame de entrada.
     * @return DataFrame con la información de error actualizada, en caso de que la columna no cumpla la validación.
     * @throws Exception si la columna especificada no existe en el DataFrame.
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

    // Comprobar por fila si la columna es int y añadir error en caso afirmativo
    val newDf = updatedDf.withColumn("arraycoderrorbyfield",
      when(col(fieldName).cast("int").isNull, array_union(
        col("arraycoderrorbyfield"),
        array(lit(s"${fieldName}: no es int"))
      )).otherwise(col("arraycoderrorbyfield"))
    )
    newDf
  }
}

