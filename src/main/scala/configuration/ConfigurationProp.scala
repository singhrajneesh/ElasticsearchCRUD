package configuration

final class ConfigurationProp (var DEFAULT_NONE: String) {
  override def toString = "Configuration Prop"
}

object ConfigurationProp {
  final val URI_SEPARATOR="/"
  final val CONFIG_FOLDER= "properties"
  final val PROPERTIES_FILE_NAME= "/Elasticsearch.properties"
}
