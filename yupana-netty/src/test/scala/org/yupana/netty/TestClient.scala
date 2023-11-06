package org.yupana.netty

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.{ ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler }
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.MessageToMessageEncoder
import org.yupana.protocol.{ Command, Frame, Hello, Response }

import java.net.InetSocketAddress
import java.util

class TestClientHandler extends SimpleChannelInboundHandler[Response[_]] {

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("ololo ololo")
    ctx.writeAndFlush(Hello(42, "HELLO VERSION", System.currentTimeMillis(), Map.empty))
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: Response[_]): Unit = {
    println(s"GOT IT $msg")
  }
}

class CommandEncoder extends MessageToMessageEncoder[Command[_]] {
  import NettyBuffer._

  override def encode(ctx: ChannelHandlerContext, msg: Command[_], out: util.List[AnyRef]): Unit = {
    val bb = Unpooled.buffer()
    Hello.readWrite.write(bb, msg.asInstanceOf[Hello])
    out.add(Frame(Hello.tag, bb.array()))
  }
}
object TestClient extends App {

  val group = new NioEventLoopGroup()
  val bs = new Bootstrap()

  bs.group(group)
    .channel(classOf[NioSocketChannel])
    .remoteAddress(new InetSocketAddress("localhost", 10101))
    .handler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline()
          .addLast(new FrameCodec())
          .addLast(new CommandEncoder)
          .addLast(new TestClientHandler)
      }
    })

  try {
    val f = bs.connect().sync()
    println("After connect")
    f.channel().closeFuture().sync()
    println("after close")
  } finally {
    group.shutdownGracefully()
  }
}