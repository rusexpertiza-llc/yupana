package org.yupana.api.types

trait InternalReaderWriter[B, V[_], S, O] extends ReaderWriter[B, V, S, O] with SizeSpecifiedReaderWriter[B, V, S, O] {}
