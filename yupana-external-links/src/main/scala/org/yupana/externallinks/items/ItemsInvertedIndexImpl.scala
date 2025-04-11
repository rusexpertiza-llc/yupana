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
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.Schema
import org.yupana.api.types.{ ID, ReaderWriter }
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.ExternalLinkService
import org.yupana.core.auth.YupanaUser
import org.yupana.core.dao.InvertedIndexDao
import org.yupana.core.model.BatchDataset
import org.yupana.core.utils.FlatAndCondition
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.serialization.ByteBufferEvalReaderWriter
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.schema.{ Dimensions, ItemDimension }

import java.nio.ByteBuffer
import scala.collection.mutable

object ItemsInvertedIndexImpl {

  val TABLE_NAME: String = "ts_items_reverse_index"

  implicit val readerWriter: ReaderWriter[ByteBuffer, ID, Int, Int] = ByteBufferEvalReaderWriter

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

  def valueSerializer(v: ItemDimension.KeyType): Array[Byte] = {
    val s = Dimensions.ITEM.rStorable.size
    val a = Array.ofDim[Byte](s)
    val bb = ByteBuffer.wrap(a)
    Dimensions.ITEM.rStorable.write(bb, v: ID[ItemDimension.KeyType])
    a
  }

  def valueDeserializer(a: Array[Byte]): ItemDimension.KeyType = {
    Dimensions.ITEM.rStorable.read(ByteBuffer.wrap(a)): ID[ItemDimension.KeyType]
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

  override def put(batchDataset: BatchDataset): Unit = {
    val items = mutable.Set.empty[String]
    batchDataset.foreach { rowNum =>
      val item = batchDataset.get[Dimensions.ITEM.T](rowNum, Dimensions.ITEM.name)
      items.add(item)
    }
    putItemNames(items.toSet)
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

  private def includeTransform(values: Seq[(SimpleCondition, String, Set[String])]): Seq[ConditionTransformation] = {
    val ids = getPhraseIds(values)
    val it = SortedSetIterator.intersectAll(ids)
    ConditionTransformation.replace(values.map(_._1), DimIdInExpr(externalLink.dimension, it))
  }

  private def excludeTransform(values: Seq[(SimpleCondition, String, Set[String])]): Seq[ConditionTransformation] = {
    val ids = getPhraseIds(values)
    val it = SortedSetIterator.unionAll(ids)
    ConditionTransformation.replace(values.map(_._1), DimIdNotInExpr(externalLink.dimension, it))
  }

  // Read only external link
  override def setLinkedValues(
      batch: BatchDataset,
      exprs: Set[LinkExpr[_]]
  ): Unit = {}

  override def transformCondition(
      condition: FlatAndCondition,
      startTime: Time,
      user: YupanaUser
  ): Seq[ConditionTransformation] = {
    ExternalLinkUtils.transformConditionT[String](
      externalLink.linkName,
      condition,
      includeTransform,
      excludeTransform
    )
  }

  private def getPhraseIds(
      fieldsValues: Seq[(SimpleCondition, String, Set[String])]
  ): Seq[SortedSetIterator[ItemDimension.KeyType]] = {
    fieldsValues.map {
      case (_, PHRASE_FIELD, phrases) => SortedSetIterator.unionAll(phrases.toSeq.map(dimIdsForPhrase))
      case (_, x, _)                  => throw new IllegalArgumentException(s"Unknown field $x")
    }
  }

  private def dimIdsForPhrase(phrase: String): SortedSetIterator[ItemDimension.KeyType] = {
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
