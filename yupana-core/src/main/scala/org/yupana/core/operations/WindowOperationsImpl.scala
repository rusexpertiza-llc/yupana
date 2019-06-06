package org.yupana.core.operations

import org.yupana.api.types.WindowOperations

trait WindowOperationsImpl extends WindowOperations {
  override def lag[T](values: Array[Option[T]], index: Int): Option[T] = if (index > 0) values(index - 1) else None
}
