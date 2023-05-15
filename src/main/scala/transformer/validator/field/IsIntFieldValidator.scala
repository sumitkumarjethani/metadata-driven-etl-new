package transformer.validator.field

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class IsIntFieldValidator extends FieldValidator {
  override def validate(fieldName: String, df: DataFrame): DataFrame = {
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

