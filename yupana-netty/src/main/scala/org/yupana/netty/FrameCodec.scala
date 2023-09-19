package org.yupana.netty

import io.netty.channel.CombinedChannelDuplexHandler

class FrameCodec
    extends CombinedChannelDuplexHandler[FrameDecoder, FrameEncoder](
      new FrameDecoder(),
      new FrameEncoder()
    ) {}
