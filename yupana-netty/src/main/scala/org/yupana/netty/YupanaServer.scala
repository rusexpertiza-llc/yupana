package org.yupana.netty

import com.typesafe.scalalogging.StrictLogging
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ ChannelInitializer, ChannelOption }

class YupanaServer(host: String, port: Int) extends StrictLogging {

  def start(): Unit = {

    val parentGroup = new NioEventLoopGroup()
    val childGroup = new NioEventLoopGroup()
    try {
      val bootstrap = new ServerBootstrap()

      bootstrap
        .group(parentGroup, childGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            ch.pipeline().addLast(new FrameDecoder())
          }
        })
        .option(ChannelOption.SO_BACKLOG, Integer.valueOf(128))
        .childOption(ChannelOption.SO_KEEPALIVE, Boolean.box(true))

      val f = bootstrap.bind(host, port).sync()
      logger.info(s"Starting YupanaServer on $host:$port")
      f.channel().closeFuture().sync()
    } finally {
      childGroup.shutdownGracefully()
      parentGroup.shutdownGracefully()
    }
  }

}

object YupanaServer {
  def main(args: Array[String]): Unit = {
    val server = new YupanaServer("localhost", 10101)

    server.start()
  }
}
