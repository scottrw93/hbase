package org.apache.hadoop.hbase.io.crypto.tls;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandler;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.OptionalSslHandler;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslHandler;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Future;
import java.util.List;

@InterfaceAudience.Private
public class DualModeSslHandler extends OptionalSslHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DualModeSslHandler.class);

  public DualModeSslHandler(SslContext sslContext) {
    super(sslContext);
  }

  @Override protected void decode(ChannelHandlerContext context, ByteBuf in, List<Object> out)
    throws Exception {
    super.decode(context, in, out);
  }

  @Override protected ChannelHandler newNonSslHandler(ChannelHandlerContext context) {
    LOG.debug("creating NON-ssl handler for channel {}", context.channel());
    return super.newNonSslHandler(context);
  }

  @Override
  protected SslHandler newSslHandler(ChannelHandlerContext context, SslContext sslContext) {
    LOG.debug("creating ssl handler for channel {}", context.channel());
    SslHandler handler = super.newSslHandler(context, sslContext);
    Future<Channel> handshakeFuture = handler.handshakeFuture();
    handshakeFuture.addListener(new CertificateVerifier(handler));
    return handler;
  }
}
