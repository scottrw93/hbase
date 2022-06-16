package org.apache.hadoop.hbase.ipc;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hbase.thirdparty.io.netty.util.AttributeKey;

public class NettyRpcServerCnxnCache  extends SimpleChannelInboundHandler<ByteBuf> {
  private final NettyRpcServer rpcServer;
  private final AttributeKey<NettyServerRpcConnection> cacheName;

  public NettyRpcServerCnxnCache(NettyRpcServer rpcServer, AttributeKey<NettyServerRpcConnection> cacheName) {
    this.rpcServer = rpcServer;
    this.cacheName = cacheName;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    NettyServerRpcConnection conn = createNettyServerRpcConnection(ctx.channel());
    ctx.channel().attr(cacheName).getAndSet(conn);
  }

  protected NettyServerRpcConnection createNettyServerRpcConnection(Channel channel) {
    return new NettyServerRpcConnection(rpcServer, channel);
  }
}
