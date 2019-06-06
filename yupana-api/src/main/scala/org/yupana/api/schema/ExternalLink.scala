package org.yupana.api.schema

/**
  * Defines external data source, which are linking to [[Table]]s.  Usually external links data mapped as one to
  * many to one of the table dimensions.  For example if you have person as a dimension, external link may contain info
  * about address, so you can query data by city.
  */
trait ExternalLink extends Serializable {
  /** Name of this external link */
  val linkName: String

  /** Attached dimension */
  val dimension: Dimension

  /** Set of field names for this link */
  val fieldsNames: Set[String]

  /** Specifies if there might be extra fields, not defined in [[org.yupana.api.schema.ExternalLink.fieldsNames]] */
  val hasDynamicFields: Boolean = false

  override def hashCode(): Int = linkName.hashCode

  override def equals(obj: Any): Boolean = obj match {
    case that: ExternalLink => this.linkName == that.linkName
    case _ => false
  }

  override def toString: String = s"ExternalLink($linkName)"
}
