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
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.TsdbBase
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.InvertedIndexDao
import org.yupana.core.utils.{ SparseTable, Table }
import org.yupana.externallinks.DimIdBasedExternalLinkService
import org.yupana.schema.Dimensions
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.utils.{ ItemsStemmer, Transliterator }

import scala.collection.mutable

object ItemsInvertedIndexImpl {

  val TABLE_NAME: String = "ts_items_reverse_index"
  val VALUE: Array[Byte] = Array.emptyByteArray
  val CACHE_NAME = "items-reverse-index"
  val CACHE_MAX_IDS_FOR_WORD = 100000

  def indexItems(items: Seq[(Long, String)]): Map[String, Seq[Long]] =
    items
      .flatMap {
        case (id, n) =>
          val words = stemmed(n)
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

  def stemmed(text: String): Seq[String] = {
    val words = ItemsStemmer.words(text)

    val separators = Set(',', '.')
    val excluded = new mutable.HashSet[String]()
    words.foreach { word =>
      separators.foreach { sep =>
        if (word.contains(sep)) {
          excluded ++= word.split(sep)
          excluded += word
        }
      }
    }

    val filtered = new mutable.ListBuffer[String]()
    words.foreach { word =>
      if (excluded.contains(word)) {
        excluded -= word
      } else {
        filtered += word
      }
      ()
    }

    filtered
      .map(Transliterator.transliterate)
      .filter(_.nonEmpty)
      .sorted
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

  def putNewItemsNames(names: Set[String]): Unit = {
    val itemIdsMap = tsdb.dictionary(Dimensions.ITEM_TAG).findIdsByValues(names)
    val newNames = names -- itemIdsMap.keySet
    putItemNames(newNames)
  }

  def putItemNames(names: Set[String]): Unit = {
    val itemIds = tsdb.dictionary(Dimensions.ITEM_TAG).getOrCreateIdsForValues(names)
    val items = itemIds.map(_.swap)
    val wordIdMap = indexItems(items.toSeq)
    invertedIndexDao.batchPut(wordIdMap.mapValues(_.toSet))
  }

  def dimIdsForStemmedWord(word: String): Iterator[Long] = {
    invertedIndexDao.values(word)
  }

  def dimIdsForStemmedWordsCached(wordWithSynonyms: (String, Set[String])): Set[Long] = wordWithSynonyms match {
    case (word, synonyms) =>
      dimIdsByStemmedWordCache
        .caching(word) {
          val dimIds = invertedIndexDao.allValues(synonyms)
          logger.info(s"synonyms: $synonyms")
          logger.info(s"found dimIds: ${dimIds.length}")
          dimIds.distinct.toArray
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

  private def getPhraseIds(fieldsValues: Seq[(String, Set[String])]): Iterator[SortedSetIterator[Long]] = {
    fieldsValues.iterator.map {
      case (PHRASE_FIELD, phrases) => SortedSetIterator.unionAll(phrases.iterator.map(dimIdsForPhrase))
      case (x, _)                  => throw new IllegalArgumentException(s"Unknown field $x")
    }
  }

  def dimIdsForPhrase(phrase: String): SortedSetIterator[Long] = {
    val words = stemmed(phrase)
    val idsPerWord = words.iterator.map { w =>
      SortedSetIterator(dimIdsForStemmedWord(w))
    }
    SortedSetIterator.intersectAll(idsPerWord)
  }
}
