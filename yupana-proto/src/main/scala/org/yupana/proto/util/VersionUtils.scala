package org.yupana.proto.util

import org.yupana.proto.Version

object VersionUtils {
  val order: Ordering[Version] = Ordering.by(v => (v.major, v.minor, v.build))
}
