package org.yupana.protocol

case class Cancel() extends Command[Cancel](Cancel)

object Cancel extends MessageHelper[Cancel] {
  override val tag: Byte = Tags.CANCEL
  override val readWrite: ReadWrite[Cancel] = ReadWrite.empty.imap(_ => Cancel())(_ => ())
}
