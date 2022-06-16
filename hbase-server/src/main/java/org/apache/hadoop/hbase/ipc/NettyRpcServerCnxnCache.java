package org.apache.hadoop.hbase.ipc;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.hbase.thirdparty.io.netty.util.AttributeKey;

@InterfaceAudience.Private
public class NettyRpcServerCnxnCache  extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRpcServerCnxnCache.class);
  private final NettyRpcServer rpcServer;
  private final AttributeKey<NettyServerRpcConnection> cacheName;

  public NettyRpcServerCnxnCache(NettyRpcServer rpcServer, AttributeKey<NettyServerRpcConnection> cacheName) {
    this.rpcServer = rpcServer;
    this.cacheName = cacheName;
  }

  @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
    NettyServerRpcConnection conn = createNettyServerRpcConnection(ctx.channel());
    ctx.channel().attr(cacheName).getAndSet(conn);
    super.channelActive(ctx);
  }

  protected NettyServerRpcConnection createNettyServerRpcConnection(Channel channel) {
    return new NettyServerRpcConnection(rpcServer, channel);
  }
}
