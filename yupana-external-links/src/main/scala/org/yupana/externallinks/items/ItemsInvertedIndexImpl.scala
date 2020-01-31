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
import org.yupana.api.query.DataPoint
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.TsdbBase
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.InvertedIndexDao
import org.yupana.core.utils.{ SparseTable, Table }
import org.yupana.externallinks.DimIdBasedExternalLinkService
import org.yupana.schema.Dimensions
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.utils.{ Tokenizer, Transliterator }

object ItemsInvertedIndexImpl {

  val TABLE_NAME: String = "ts_items_reverse_index"

  def indexItems(items: Seq[(Long, String)]): Map[String, Seq[Long]] =
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
    tsdb: TsdbBase,
    invertedIndexDao: InvertedIndexDao[String, Long],
    override val externalLink: ItemsInvertedIndex
) extends DimIdBasedExternalLinkService[ItemsInvertedIndex](tsdb)
    with StrictLogging {

  import ItemsInvertedIndexImpl._
  import externalLink._

  private val dimIdsByStemmedWordCache = CacheFactory.initCache[String, Array[Long]]("dim_ids_by_word")

  override def put(dataPoints: Seq[DataPoint]): Unit = {
    println("ItemsInvertedIndexImpl put!")
    val items = dataPoints.flatMap(dp => dp.dimensions.get(Dimensions.ITEM_TAG)).toSet
    putItemNames(items)
  }

  def putItemNames(names: Set[String]): Unit = {
    val itemIds = tsdb.dictionary(Dimensions.ITEM_TAG).getOrCreateIdsForValues(names)
    val items = itemIds.map(_.swap)
    val wordIdMap = indexItems(items.toSeq)
    invertedIndexDao.batchPut(wordIdMap.mapValues(_.toSet))
  }

  def dimIdsForStemmedWord(word: String): SortedSetIterator[Long] = {
    invertedIndexDao.values(word)
  }

  def dimIdsForPrefix(prefix: String): SortedSetIterator[Long] = {
    invertedIndexDao.valuesByPrefix(prefix)
  }

  def dimIdsForStemmedWordsCached(word: String, synonyms: Set[String]): Set[Long] = {
    dimIdsByStemmedWordCache
      .caching(word) {
        val dimIds = invertedIndexDao.allValues(synonyms)
        logger.info(s"synonyms: $synonyms")
        logger.info(s"found dimIds: ${dimIds.length}")
        dimIds.toArray
      }
      .toSet
  }

  override def dimIdsForAllFieldsValues(fieldsValues: Seq[(String, Set[String])]): SortedSetIterator[Long] = {
    val ids = getPhraseIds(fieldsValues)
    SortedSetIterator.intersectAll(ids)
  }

  override def dimIdsForAnyFieldsValues(fieldsValues: Seq[(String, Set[String])]): SortedSetIterator[Long] = {
    val ids = getPhraseIds(fieldsValues)
    SortedSetIterator.unionAll(ids)
  }

  override def fieldValuesForDimIds(fields: Set[String], dimIds: Set[Long]): Table[Long, String, String] = {
    SparseTable.empty
  }

  private def getPhraseIds(fieldsValues: Seq[(String, Set[String])]): Seq[SortedSetIterator[Long]] = {
    fieldsValues.map {
      case (PHRASE_FIELD, phrases) => SortedSetIterator.unionAll(phrases.toSeq.map(dimIdsForPhrase))
      case (x, _)                  => throw new IllegalArgumentException(s"Unknown field $x")
    }
  }

  def dimIdsForPhrase(phrase: String): SortedSetIterator[Long] = {
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
