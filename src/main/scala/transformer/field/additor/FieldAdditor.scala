package transformer.field.additor

import org.apache.spark.sql.DataFrame

trait FieldAdditor {
  def add(df: DataFrame): DataFrame
}
