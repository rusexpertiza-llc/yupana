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

package org.yupana.core.utils

import com.twitter.algebird.{ Hash128, MurmurHash128 }
import org.yupana.api.Time

object Hash128Utils {
  implicit lazy val timeHash: Hash128[Time] = new Hash128[Time] {
    override val DefaultSeed: Long = Hash128.DefaultSeed
    override def hashWithSeed(seed: Long, k: Time): (Long, Long) = MurmurHash128(seed)(k.millis)
  }
}
