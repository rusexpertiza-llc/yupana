package org.yupana.postgres

import com.typesafe.scalalogging.StrictLogging
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ Channel, ChannelFuture, ChannelInitializer, ChannelOption }
import io.netty.handler.timeout.IdleStateHandler

import scala.concurrent.{ Future, Promise }

class YupanaPostgres(host: String, port: Int, nThreads: Int, serverContext: ServerContext) extends StrictLogging {

  private var channel: Channel = _
  def start(): Future[Unit] = {
    if (channel != null) throw new IllegalStateException("Already started")

    val closePromise = Promise[Unit]()
    val parentGroup = new NioEventLoopGroup()
    val childGroup = new NioEventLoopGroup()
    val yupanaGroup = new NioEventLoopGroup(nThreads)
    val bootstrap = new ServerBootstrap()

    bootstrap
      .group(parentGroup, childGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline().addLast(new IdleStateHandler(30, 0, 0))
          ch.pipeline().addLast("decoder", new InitialMessageDecoder())
          ch.pipeline().addLast("encoder", new MessageEncoder())
          ch.pipeline().addLast(yupanaGroup, "handler", new ConnectingHandler(serverContext))
        }
      })
      .option(ChannelOption.SO_BACKLOG, Integer.valueOf(128))
      .childOption(ChannelOption.SO_KEEPALIVE, Boolean.box(true))

    val f = bootstrap.bind(host, port).sync()
    logger.info(s"Starting YupanaServer on $host:$port")
    channel = f.channel()
    f.channel()
      .closeFuture()
      .addListener((_: ChannelFuture) => {
        yupanaGroup.shutdownGracefully()
        childGroup.shutdownGracefully()
        parentGroup.shutdownGracefully()
        closePromise.success(())
      })

    closePromise.future
  }

  def stop(): Unit = {
    if (channel != null) {
      channel.close().sync()
      channel = null
    }
  }
}
