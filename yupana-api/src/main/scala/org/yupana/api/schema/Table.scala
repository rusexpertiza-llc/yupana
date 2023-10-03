/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.api.schema

import scala.collection.mutable

/**
  * Table content definition.
  *
  * @param name name of this table
  * @param rowTimeSpan duration of time series in milliseconds.
  * @param dimensionSeq sequence of dimensions for this table.
  * @param metrics metrics description for each data point of this table.
  * @param externalLinks external links applicable for this table.
  * @param epochTime milliseconds elapsed since the Unix epoch before the beginning of time series
  */
class Table(
    val id: Byte,
    val name: String,
    val rowTimeSpan: Long,
    val dimensionSeq: Seq[Dimension],
    val metrics: Seq[Metric],
    val externalLinks: Seq[ExternalLink],
    val epochTime: Long
) extends Serializable {

  private lazy val dimensionTagsMap = {
    val dimTags = dimensionSeq.zipWithIndex.map { case (dim, idx) => (dim, (Table.DIM_TAG_OFFSET + idx).toByte) }
    mutable.Map(dimTags: _*)
  }

  private lazy val tagFields: Array[Option[Either[Metric, Dimension]]] = {
    val tagFields = Array.fill[Option[Either[Metric, Dimension]]](255)(None)

    metrics.foreach { m =>
      tagFields(m.tag & 0xFF) = Some(Left(m))
    }

    dimensionSeq.foreach { dim =>
      tagFields(dimensionTag(dim) & 0xFF) = Some(Right(dim))
    }

    tagFields
  }

  val metricTagsSet: Set[Byte] = metrics.map(_.tag).toSet

  @inline
  final def fieldForTag(tag: Byte): Option[Either[Metric, Dimension]] = tagFields(tag & 0xFF)

  @inline
  def dimensionTag(dimension: Dimension): Byte = {
    dimensionTagsMap(dimension)
  }

  @inline
  def dimensionTagExists(dimension: Dimension): Boolean = {
    dimensionTagsMap.contains(dimension)
  }

  override def toString: String = s"Table($name)"

  def withExternalLinks(extraLinks: Seq[ExternalLink]): Table = {
    new Table(id, name, rowTimeSpan, dimensionSeq, metrics, externalLinks ++ extraLinks, epochTime)
  }

  def withExternalLinkReplaced[O <: ExternalLink, N <: O](oldExternalLink: O, newExternalLink: N): Table = {
    if (!externalLinks.contains(oldExternalLink)) {
      throw new IllegalArgumentException(s"Unsupported external link ${oldExternalLink.linkName} for table $name")
    }

    if (newExternalLink.linkName != oldExternalLink.linkName) {
      throw new IllegalArgumentException(
        s"Replacing link ${oldExternalLink.linkName} and replacement ${newExternalLink.linkName} must have same names"
      )
    }

    val unsupportedFields = oldExternalLink.fields -- newExternalLink.fields

    if (unsupportedFields.nonEmpty) {
      throw new IllegalArgumentException(
        s"Fields ${unsupportedFields.mkString(",")} are not supported in new catalog ${newExternalLink.linkName}"
      )
    }

    new Table(
      id,
      name,
      rowTimeSpan,
      dimensionSeq,
      metrics,
      externalLinks.filter(_ != oldExternalLink) :+ newExternalLink,
      epochTime
    )
  }

  def withMetrics(extraMetrics: Seq[Metric]): Table = {
    new Table(id, name, rowTimeSpan, dimensionSeq, metrics ++ extraMetrics, externalLinks, epochTime)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: Table => this.name == that.name
      case _           => false
    }
  }

  override def hashCode(): Int = name.hashCode
}

object Table {
  val MAX_TAGS: Int = 256
  val TIME_FIELD_NAME: String = "time"
  val DIM_TAG_OFFSET: Byte = 214.toByte
}
