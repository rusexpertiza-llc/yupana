//package org.yupana.netty
//
//import io.netty.channel.ChannelHandlerContext
//import io.netty.handler.codec.MessageToMessageCodec
//import org.yupana.netty.protocol.{ Command, Response }
//
//import java.util
//
//class MessageHandler extends MessageToMessageCodec[Frame, Command] {
//  override def encode(ctx: ChannelHandlerContext, msg: Response, out: util.List[AnyRef]): Unit = ???
//
//  override def decode(ctx: ChannelHandlerContext, msg: Command, out: util.List[AnyRef]): Unit = ???
//}
