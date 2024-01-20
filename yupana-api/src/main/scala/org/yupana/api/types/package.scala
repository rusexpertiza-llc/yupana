package org.yupana.api

package object types {
  type ID[T] = T
  type TypedInt[_] = Int

  type ByteReaderWriter[B] = ReaderWriter[B, ID, TypedInt]
}
