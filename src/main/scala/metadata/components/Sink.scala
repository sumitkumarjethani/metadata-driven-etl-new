package metadata.components

import metadata.components.types.{FormatType, SaveModeType, SaveStatusType}

case class Sink(input: String, status: SaveStatusType.SaveStatusType, name: String, paths: List[String],
                format: FormatType.FormatType, saveMode: SaveModeType.SaveModeType)