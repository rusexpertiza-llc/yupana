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

package org.yupana.core.auth

trait Permission[+S, +A] {
  def implies[SS >: S, AA >: A](subject: SS, action: AA): Boolean
}

object Permission {
  case object All extends Permission[Nothing, Nothing] {
    override def implies[SS, AA](subject: SS, action: AA): Boolean = true
  }

  case object Nothing extends Permission[Nothing, Nothing] {
    override def implies[SS, AA](subject: SS, action: AA): Boolean = false
  }

  case class One[S, A](subject: S, action: A) extends Permission[S, A] {
    override def implies[SS >: S, AA >: A](subject: SS, action: AA): Boolean =
      this.subject == subject && this.action == action
  }
}
