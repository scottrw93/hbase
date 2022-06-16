package org.apache.hadoop.hbase.ipc;

import static org.apache.hadoop.hbase.ipc.NettyRpcServer.CONNECTION_ATTRIBUTE;
import java.security.Principal;
import java.security.cert.CertificateException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelFutureListener;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslHandler;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Future;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.GenericFutureListener;

@InterfaceAudience.Private
public class NettyRpcServerSslCertificateVerifier implements GenericFutureListener<Future<Channel>> {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRpcServerSslCertificateVerifier.class);

  private final SslHandler sslHandler;
  private String requiredCommonNameString;

  NettyRpcServerSslCertificateVerifier(SslHandler sslHandler, String requiredCommonNameString) {
    this.sslHandler = sslHandler;
    this.requiredCommonNameString = requiredCommonNameString;
  }

  @Override
  public void operationComplete(Future<Channel> future) throws Exception {
    if (future.isSuccess()) {
      LOG.debug("Successful handshake, trying to authenticate");
      SSLEngine engine = sslHandler.engine();
      SSLSession session = engine.getSession();
      if (engine.getNeedClientAuth() || engine.getWantClientAuth()) {
        try {
          Principal principal;
          try {
            principal = session.getPeerPrincipal();
          } catch (SSLPeerUnverifiedException e) {
            if (engine.getNeedClientAuth()) {
              sendErrorAndClose(future, "No certificate available, but client auth is required");
            }
            return;
          }

          LOG.debug("Got principal of type {} for channel {}: {}",
            principal.getClass().getSimpleName(), future.getNow().id(), principal.getName());
          LdapName name = new LdapName(principal.getName());
          for (Rdn rdn : name.getRdns()) {
            if (rdn.getType().equals("CN")) {
              LOG.debug("Found CN {}", rdn.getValue());
              LOG.debug("Looking for {}", requiredCommonNameString);
              if (rdn.getValue().toString().contains(requiredCommonNameString)) {
                return;
              }
            }
            sendErrorAndClose(future, "Missing proper CN to access this cluster");
          }
        } catch (Exception e) {
          sendErrorAndClose(future, "Failed to authenticate: " + e.getMessage());
          LOG.debug("Failed to authenticate", e);
        }
      }
    }
  }

  private void sendErrorAndClose(Future<Channel> future, String errorMessage) {
    Channel channel = future.getNow();
    LOG.debug("Returning error to channel {}: {}", channel.id(), errorMessage);
    NettyServerRpcConnection connection = channel.attr(CONNECTION_ATTRIBUTE).get();
    NettyServerCall authError = connection.createCall(0, connection.service, null,
      null, null, null, 0, connection.addr, 0, null);
    authError.setResponse(null, null, new DoNotRetryIOException(errorMessage), errorMessage);
    channel.writeAndFlush(authError).addListener(ChannelFutureListener.CLOSE);
  }
}
