package transformer.field.additor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp}

class FieldAdditionCurrentTimestamp(fieldName: String) extends FieldAddition {
  override def add(df: DataFrame): DataFrame = {
    df.withColumn(fieldName, current_timestamp())
  }
}
