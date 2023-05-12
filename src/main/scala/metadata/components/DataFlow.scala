package metadata.components

case class DataFlow(name: String, sources: List[Source],
                    transformations: List[Transformation],
                    sinks: List[Sink])