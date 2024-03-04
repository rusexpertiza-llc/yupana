package org.yupana.postgres

import com.typesafe.scalalogging.StrictLogging
import io.netty.channel.{ ChannelHandlerContext, SimpleChannelInboundHandler }
import org.yupana.postgres.protocol.{ Message, No, SSLRequest }

class ConnectingHandler(context: ServerContext) extends SimpleChannelInboundHandler[Message] with StrictLogging {
  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    logger.info(s"GOT A MESSAGE $msg")
    msg match {
      case SSLRequest =>
        ctx.writeAndFlush(No)
      case _ => ???
    }
  }
}
