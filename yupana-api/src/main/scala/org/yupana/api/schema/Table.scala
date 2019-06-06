package org.yupana.api.schema

/**
  * Table content definition.
  * @param name name of this table
  * @param rowTimeSpan duration of time series in milliseconds.
  * @param dimensionSeq sequence of dimensions for this table.
  * @param metrics metrics description for each data point of this table.
  * @param externalLinks external links applicable for this table.
  */
class Table(val name: String,
            val rowTimeSpan: Long,
            val dimensionSeq: Seq[Dimension],
            val metrics: Seq[Metric],
            val externalLinks: Seq[ExternalLink]
           ) extends Serializable {
  override def toString: String = s"Table($name)"

  def withExternalLinks(extraLinks: Seq[ExternalLink]): Table = {
    new Table(name, rowTimeSpan, dimensionSeq, metrics, externalLinks ++ extraLinks)
  }

  def withMetrics(extraMetrics: Seq[Metric]): Table = {
    new Table(name, rowTimeSpan, dimensionSeq, metrics ++ extraMetrics, externalLinks)
  }
}

object Table {
  val TIME_FIELD_NAME: String = "time"
}
