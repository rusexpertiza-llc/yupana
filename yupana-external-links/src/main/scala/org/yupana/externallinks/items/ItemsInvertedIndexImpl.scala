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
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.ExternalLinkService
import org.yupana.core.dao.InvertedIndexDao
import org.yupana.core.model.InternalRow
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.schema.{ Dimensions, ItemDimension }
import org.yupana.utils.{ Tokenizer, Transliterator }

object ItemsInvertedIndexImpl {

  val TABLE_NAME: String = "ts_items_reverse_index"

  def indexItems(items: Seq[(ItemDimension.KeyType, String)]): Map[String, Seq[ItemDimension.KeyType]] =
    items
      .flatMap {
        case (id, n) =>
          val words = Tokenizer.transliteratedTokens(n)
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
    val wordIdMap = indexItems(items)
    invertedIndexDao.batchPut(wordIdMap.mapValues(_.toSet))
  }

  def dimIdsForStemmedWord(word: String): SortedSetIterator[ItemDimension.KeyType] = {
    invertedIndexDao.values(word)
  }

  def dimIdsForPrefix(prefix: String): SortedSetIterator[ItemDimension.KeyType] = {
    invertedIndexDao.valuesByPrefix(prefix)
  }

  private def includeCondition(values: Seq[(String, Set[String])]): Condition = {
    val ids = getPhraseIds(values)
    val it = SortedSetIterator.intersectAll(ids)
    DimIdInExpr(externalLink.dimension, it)
  }

  private def excludeCondition(values: Seq[(String, Set[String])]): Condition = {
    val ids = getPhraseIds(values)
    val it = SortedSetIterator.unionAll(ids)
    DimIdNotInExpr(externalLink.dimension, it)
  }

  // Read only external link
  override def setLinkedValues(
      exprIndex: collection.Map[Expression, Int],
      rows: Seq[InternalRow],
      exprs: Set[LinkExpr]
  ): Unit = {}

  override def condition(condition: Condition): Condition = {
    ExternalLinkUtils.transformCondition(externalLink.linkName, condition, includeCondition, excludeCondition)
  }

  private def getPhraseIds(fieldsValues: Seq[(String, Set[String])]): Seq[SortedSetIterator[ItemDimension.KeyType]] = {
    fieldsValues.map {
      case (PHRASE_FIELD, phrases) => SortedSetIterator.unionAll(phrases.toSeq.map(dimIdsForPhrase))
      case (x, _)                  => throw new IllegalArgumentException(s"Unknown field $x")
    }
  }

  def dimIdsForPhrase(phrase: String): SortedSetIterator[ItemDimension.KeyType] = {
    val (prefixes, words) = phrase.split(' ').partition(_.endsWith("%"))

    val stemmedWords = words.map(Tokenizer.stem).map(Transliterator.transliterate)

    val idsPerWord = stemmedWords.map(dimIdsForStemmedWord)

    val transPrefixes = prefixes
      .map(s => s.substring(0, s.length - 1).trim.toLowerCase)
      .filter(_.nonEmpty)
      .map(Transliterator.transliterate)
    val idsPerPrefix = transPrefixes.map(dimIdsForPrefix)
    SortedSetIterator.intersectAll(idsPerWord ++ idsPerPrefix)
  }
}
