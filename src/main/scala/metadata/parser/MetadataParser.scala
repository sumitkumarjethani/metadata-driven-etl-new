package metadata.parser
import metadata.components.DataFlow

trait MetadataParser {
  def parse(metadata: Any): DataFlow
}
