package org.yupana.api.schema

trait Table extends Serializable {
  val name: String
  val rowTimeSpan: Long
  val tagsSeq: Seq[String]
  val fields: Seq[Measure]
  var catalogs: Seq[ExternalLink]
  val rollups: Seq[Rollup]

  override def toString: String = {
    s"Table($name)"
  }

  lazy val fieldGroups: Set[Int] = fields.map(_.group).toSet
}

object Table {
  val TIME_FIELD_NAME: String = "time"
}
