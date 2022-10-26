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

package org.yupana.externallinks.items

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.Schema
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.ExternalLinkService
import org.yupana.core.dao.InvertedIndexDao
import org.yupana.core.model.InternalRow
import org.yupana.core.utils.FlatAndCondition
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.schema.{ Dimensions, ItemDimension }

object ItemsInvertedIndexImpl {

  val TABLE_NAME: String = "ts_items_reverse_index"

  def indexItems(schema: Schema)(items: Seq[(ItemDimension.KeyType, String)]): Map[String, Seq[ItemDimension.KeyType]] =
    items
      .flatMap {
        case (id, n) =>
          val words = schema.tokenizer.transliteratedTokens(schema.itemFixer.fix(n))
          words.map(_ -> id)
      }
      .groupBy {
        case (word, _) =>
          word
      }
      .map {
        case (word, group) =>
          (word, group.map(_._2))
      }
}

class ItemsInvertedIndexImpl(
    override val schema: Schema,
    invertedIndexDao: InvertedIndexDao[String, ItemDimension.KeyType],
    override val putEnabled: Boolean,
    override val externalLink: ItemsInvertedIndex
) extends ExternalLinkService[ItemsInvertedIndex]
    with StrictLogging {

  import ItemsInvertedIndexImpl._
  import externalLink._

  override def put(dataPoints: Seq[DataPoint]): Unit = {
    if (putEnabled) {
      val items = dataPoints
        .flatMap(dp => dp.dimensionValue(Dimensions.ITEM))
        .toSet
        .filter(_.trim.nonEmpty)
      putItemNames(items)
    }
  }

  def putItemNames(names: Set[String]): Unit = {
    val items = names.map(n => Dimensions.ITEM.hashFunction(n) -> n).toSeq
    val wordIdMap = indexItems(schema)(items)
    invertedIndexDao.batchPut(wordIdMap.map { case (k, v) => k -> v.toSet })
  }

  def dimIdsForStemmedWord(word: String): SortedSetIterator[ItemDimension.KeyType] = {
    invertedIndexDao.values(word)
  }

  def dimIdsForPrefix(prefix: String): SortedSetIterator[ItemDimension.KeyType] = {
    invertedIndexDao.valuesByPrefix(prefix)
  }

  private def includeTransform(values: Seq[(Condition, String, Set[String])]): TransformCondition = {
    val ids = getPhraseIds(values)
    val it = SortedSetIterator.intersectAll(ids)
    Replace(
      values.map(_._1).toSet,
      DimIdInExpr(externalLink.dimension, it)
    )
  }

  private def excludeTransform(values: Seq[(Condition, String, Set[String])]): TransformCondition = {
    val ids = getPhraseIds(values)
    val it = SortedSetIterator.unionAll(ids)
    Replace(
      values.map(_._1).toSet,
      DimIdNotInExpr(externalLink.dimension, it)
    )
  }

  // Read only external link
  override def setLinkedValues(
      exprIndex: collection.Map[Expression[_], Int],
      rows: Seq[InternalRow],
      exprs: Set[LinkExpr[_]]
  ): Unit = {}

  override def transformCondition(condition: FlatAndCondition): Seq[TransformCondition] = {
    ExternalLinkUtils.transformConditionT[String](
      externalLink.linkName,
      condition,
      includeTransform,
      excludeTransform
    )
  }

  private def getPhraseIds(
      fieldsValues: Seq[(Condition, String, Set[String])]
  ): Seq[SortedSetIterator[ItemDimension.KeyType]] = {
    fieldsValues.map {
      case (_, PHRASE_FIELD, phrases) => SortedSetIterator.unionAll(phrases.toSeq.map(dimIdsForPhrase))
      case (_, x, _)                  => throw new IllegalArgumentException(s"Unknown field $x")
    }
  }

  def dimIdsForPhrase(phrase: String): SortedSetIterator[ItemDimension.KeyType] = {
    val (prefixes, words) = phrase.split(' ').partition(_.endsWith("%"))

    val stemmedWords = words.flatMap(schema.tokenizer.transliteratedTokens)

    val idsPerWord = stemmedWords.map(dimIdsForStemmedWord)

    val transPrefixes = prefixes
      .map(s => s.substring(0, s.length - 1).trim.toLowerCase)
      .filter(_.nonEmpty)
      .map(schema.transliterator.transliterate)
    val idsPerPrefix = transPrefixes.map(dimIdsForPrefix)
    SortedSetIterator.intersectAll(idsPerWord.toSeq ++ idsPerPrefix)
  }
}
