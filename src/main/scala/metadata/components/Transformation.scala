package metadata.components

import metadata.components.types.TransformationType

case class Transformation(name: String, `type`: TransformationType.TransformationType, params: Map[String, Any])