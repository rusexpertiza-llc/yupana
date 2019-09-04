package org.yupana.externallinks.items

import com.typesafe.scalalogging.StrictLogging
import org.yupana.core.TsdbBase
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.InvertedIndexDao
import org.yupana.core.utils.{CollectionUtils, SparseTable, Table}
import org.yupana.externallinks.DimIdBasedExternalLinkService
import org.yupana.schema.Dimensions
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.utils.{ItemsStemmer, Transliterator}

import scala.collection.mutable

object ItemsInvertedIndexImpl {

  val TABLE_NAME: String = "ts_items_reverse_index"
  val VALUE: Array[Byte] = Array.emptyByteArray
  val CACHE_NAME = "items-reverse-index"
  val CACHE_MAX_IDS_FOR_WORD = 100000

  def indexItems(items: Seq[(Long, String)]): Map[String, Seq[Long]] =
    items.flatMap { case (id, n) =>
      val words = stemmed(n)
      words.map(_ -> id)
    }.groupBy { case (word, _) =>
      word
    }.map { case (word, group) =>
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

class ItemsInvertedIndexImpl(tsdb: TsdbBase, invertedIndexDao: InvertedIndexDao[String, Long], override val externalLink: ItemsInvertedIndex)
  extends DimIdBasedExternalLinkService[ItemsInvertedIndex](tsdb) with StrictLogging {

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
      dimIdsByStemmedWordCache.caching(word) {
        val dimIds = invertedIndexDao.allValues(synonyms)
        logger.info(s"synonyms: $synonyms")
        logger.info(s"found dimIds: ${dimIds.length}")
        dimIds.distinct.toArray
      }.toSet
  }

  override def dimIdsForAllFieldsValues(fieldsValues: Seq[(String, Set[String])]): Set[Long] = {
    val ids = getPhraseIds(fieldsValues)
    CollectionUtils.intersectAll(ids)
  }

  override def dimIdsForAnyFieldsValues(fieldsValues: Seq[(String, Set[String])]): Set[Long] = {
    val ids = getPhraseIds(fieldsValues)
    ids.foldLeft(Set.empty[Long])(_ union _)
  }

  override def fieldValuesForDimIds(fields: Set[String], dimIds: Set[Long]): Table[Long, String, String] = {
    SparseTable.empty
  }

  private def getPhraseIds(fieldsValues: Seq[(String, Set[String])]): Seq[Set[Long]] = {
    fieldsValues.map {
      case (PHRASE_FIELD, phrases) => phrases.flatMap(dimIdsForPhrase)
      case (x, _) => throw new IllegalArgumentException(s"Unknown field $x")
    }
  }

  def dimIdsForPhrase(phrase: String): Seq[Long] = {
    val words = stemmed(phrase)
    val idsPerWord = words.map { w =>
      dimIdsForStemmedWord(w).toSet
    }

    if (idsPerWord.nonEmpty) {
      idsPerWord.reduceLeft((a, b) => a intersect b).toSeq
    } else Seq.empty
  }
}
