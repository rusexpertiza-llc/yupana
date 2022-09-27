package org.yupana.core.utils

import com.twitter.algebird.{ Hash128, MurmurHash128 }
import org.yupana.api.Time

object Hash128Utils {
  implicit lazy val timeHash: Hash128[Time] = new Hash128[Time] {
    override val DefaultSeed: Long = Hash128.DefaultSeed
    override def hashWithSeed(seed: Long, k: Time): (Long, Long) = MurmurHash128(seed)(k.millis)
  }
}
