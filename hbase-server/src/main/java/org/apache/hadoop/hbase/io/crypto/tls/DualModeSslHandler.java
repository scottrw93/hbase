package org.apache.hadoop.hbase.io.crypto.tls;

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

@InterfaceAudience.Private
public class DualModeSslHandler extends OptionalSslHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DualModeSslHandler.class);
  private String requiredCommonNameString;

  public DualModeSslHandler(SslContext sslContext, String requiredCommonNameString) {
    super(sslContext);
    this.requiredCommonNameString = requiredCommonNameString;
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
    handshakeFuture.addListener(new CertificateVerifier(handler, requiredCommonNameString));
    return handler;
  }
}
