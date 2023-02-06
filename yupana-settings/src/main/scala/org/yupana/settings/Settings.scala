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

package org.yupana.settings

import com.typesafe.scalalogging.StrictLogging

import java.util.Properties

case class SettingsException(cause: Throwable, key: String, maybeValue: Option[String] = None)
    extends RuntimeException(
      s"""Failed to read setting for key $key${maybeValue.map(v => s" => $v").getOrElse("")}""",
      cause
    )

abstract class Settings { self =>

  def getByKey(k: String): Option[String]

  protected def format(k: String): String = k

  def apply[T: Read](key: String): T =
    apply(key, throw SettingsException(new IllegalArgumentException(s"Setting ${format(key)} not found"), format(key)))

  def apply[T: Read](key: String, default: => T): T = opt(key).getOrElse(default)

  def readLines[T: Read](k: String, default: => Set[T]): Set[T] = {
    opt[String](k) match {
      case Some(value) =>
        value
          .split("\r?\n|\r")
          .toSeq
          .map(_.trim)
          .map { vStr =>
            try {
              implicitly[Read[T]].read(vStr)
            } catch {
              case t: Throwable => throw SettingsException(t, k, Some(vStr))
            }
          }
          .toSet
      case None =>
        default
    }
  }

  def exists(key: String): Boolean = getByKey(key).isDefined

  def opt[T: Read](key: String, allowEmpty: Boolean = false): Option[T] = {
    getByKey(key)
      .map(_.trim)
      .filter(vStr => vStr.nonEmpty || allowEmpty)
      .map { vStr =>
        try {
          implicitly[Read[T]].read(vStr)
        } catch {
          case t: Throwable => throw SettingsException(t, key, Some(vStr))
        }
      }
  }

  def inner(prefix: String): Settings = new Settings {
    override def format(k: String): String = prefix + k
    override def getByKey(k: String): Option[String] = self.getByKey(prefix + k)
  }

  def settingToString(k: String, v: Option[String]): String = {
    v match {
      case Some(_) if k.contains("pass") =>
        s"$k = ******"
      case Some(x) =>
        s"$k = $x"
      case None =>
        s"$k is not defined"
    }
  }
}

object Settings extends StrictLogging {

  def apply(props: Properties): Settings = {
    new PropertiesSettings(props)
  }

  class PropertiesSettings(props: Properties) extends Settings with Serializable {
    override def getByKey(k: String): Option[String] = {
      val v = Option(props.getProperty(k))
      logger.info("read setting value: " + settingToString(k, v))
      v
    }
  }

}
