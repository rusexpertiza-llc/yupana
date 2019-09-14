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

  def withExternalLinkReplaced[O <: ExternalLink, N <: O](oldExternalLink: O, newExternalLink: N): Table = {
    if (!externalLinks.contains(oldExternalLink)) {
      throw new IllegalArgumentException(s"Unsupported external link ${oldExternalLink.linkName} for table $name")
    }

    if (newExternalLink.linkName != oldExternalLink.linkName) {
      throw new IllegalArgumentException(s"Replacing link ${oldExternalLink.linkName} and replacement ${newExternalLink.linkName} must have same names")
    }

    val unsupportedFields = oldExternalLink.fieldsNames -- newExternalLink.fieldsNames

    if (unsupportedFields.nonEmpty) {
      throw new IllegalArgumentException(s"Fields ${unsupportedFields.mkString(",")} are not supported in new catalog ${newExternalLink.linkName}")
    }

    new Table(name, rowTimeSpan, dimensionSeq, metrics, externalLinks.filter(_ != oldExternalLink) :+ newExternalLink)
  }

  def withMetrics(extraMetrics: Seq[Metric]): Table = {
    new Table(name, rowTimeSpan, dimensionSeq, metrics ++ extraMetrics, externalLinks)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: Table => this.name == that.name
      case _ => false
    }
  }

  override def hashCode(): Int = name.hashCode
}

object Table {
  val TIME_FIELD_NAME: String = "time"
}
