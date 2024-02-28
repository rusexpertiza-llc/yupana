package org.yupana.postgres

import com.typesafe.scalalogging.StrictLogging
import io.netty.buffer.Unpooled
import io.netty.channel.{ ChannelHandlerContext, SimpleChannelInboundHandler }
import org.yupana.postgres.protocol.{ Message, SSLRequest }

class ConnectingHandler(context: ServerContext) extends SimpleChannelInboundHandler[Message] with StrictLogging {
  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    logger.info(s"GOT A MESSAGE $msg")
    msg match {
      case SSLRequest =>
        val bb = Unpooled.buffer()
        bb.writeByte('N'.toByte)
        ctx.writeAndFlush(bb)
      case _ => ???
    }
  }
}
