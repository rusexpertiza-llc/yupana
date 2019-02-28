package org.yupana.api.schema

trait Table extends Serializable {
  val name: String
  val rowTimeSpan: Long
  val dimensionSeq: Seq[String]
  val measures: Seq[Measure]
  var externalLinks: Seq[ExternalLink]
  val rollups: Seq[Rollup]

  override def toString: String = {
    s"Table($name)"
  }

  lazy val fieldGroups: Set[Int] = measures.map(_.group).toSet
}

object Table {
  val TIME_FIELD_NAME: String = "time"
}
