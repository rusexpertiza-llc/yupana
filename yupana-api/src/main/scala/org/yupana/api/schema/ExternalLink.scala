package org.yupana.api.schema

trait ExternalLink extends Serializable {
  val linkName: String
  val dimensionName: String
  val fieldsNames: Set[String]
  val hasDynamicFields: Boolean = false

  override def toString: String = s"ExternalLink($linkName)"
}
