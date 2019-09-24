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

package org.yupana.math

import scala.annotation.tailrec

object KMeans {

  def perform(xs: Seq[Long], k: Int): Map[Long, Seq[Long]] = {
    val means = initMeans(xs, k)
    kMeans(xs, means, 1)
  }

  @tailrec
  private def kMeans(xs: Seq[Long], means: Seq[Long], eta: Double): Map[Long, Seq[Long]] = {
    val partitions = partition(xs, means)
    val newMeans = updateMeans(partitions)
    if (converged(eta)(means, newMeans)) {
      partitions
    } else {
      kMeans(xs, newMeans, eta)
    }
  }

  private def initMeans(xs: Seq[Long], k: Int) = {
    val rand = new scala.util.Random(7)

    def nextMean(means: Seq[Long]): Seq[Long] = {

      if (means.size == k || means.size == xs.size) {
        means
      } else {

        val sumDx2 = xs.foldLeft(0d) { (s, x) =>
          s + dx2(x, means)
        }

        val rndSum = rand.nextFloat() * sumDx2

        val mean = xs.iterator
          .scanLeft(0L -> 0d) {
            case ((_, sum), x) =>
              x -> (sum + dx2(x, means))
          }
          .find { case (_, sum) => sum > 0 && sum > rndSum }
          .get
          ._1

        nextMean(means :+ mean)
      }

    }
    val startMean = xs(rand.nextInt(xs.length))
    nextMean(Seq(startMean))
  }

  private def dx2(x: Long, means: Seq[Long]) = {
    val d = findClosest(x, means) - x
    d.toDouble * d
  }

  private def findClosest(x: Long, means: Seq[Long]): Long = {
    means.minBy(m => math.abs(m - x))
  }

  private def partition(xs: Seq[Long], means: Seq[Long]): Map[Long, Seq[Long]] = {
    xs.groupBy(x => findClosest(x, means))
  }

  private def updateMeans(partitions: Map[Long, Seq[Long]]): Seq[Long] = {
    partitions.map {
      case (mean, xs) =>
        xs.sum / xs.size
    }.toSeq
  }

  private def converged(eta: Double)(oldMeans: Seq[Long], newMeans: Seq[Long]): Boolean = {
    oldMeans.zip(newMeans).forall { case (om, nm) => math.abs(om - nm) <= eta }
  }
}
