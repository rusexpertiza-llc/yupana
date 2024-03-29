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

package org.yupana.spark

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.yupana.settings.Settings

import scala.util.Try

case class SparkConfSettings(sc: SparkConf) extends Settings with Serializable with StrictLogging {
  override def getByKey(k: String): Option[String] = {
    val v = Try(sc.get(k)).toOption
    logger.info("read setting value: " + settingToString(k, v))
    v
  }
}
