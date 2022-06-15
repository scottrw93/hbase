package org.apache.hadoop.hbase.io.crypto.tls;

import java.security.Principal;
import java.security.cert.CertificateException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslHandler;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Future;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.GenericFutureListener;

@InterfaceAudience.Private
public class CertificateVerifier implements GenericFutureListener<Future<Channel>> {
  private static final Logger LOG = LoggerFactory.getLogger(CertificateVerifier.class);

  private final SslHandler sslHandler;
  private String requiredCommonNameString;

  CertificateVerifier(SslHandler sslHandler, String requiredCommonNameString) {
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
              throw new CertificateException("No certificate available, but client auth is required");
            }
            return;
          }

          LOG.debug("Got principal for session {}", principal);
          LdapName name = new LdapName(principal.getName());
          for (Rdn rdn : name.getRdns()) {
            if (rdn.getType().equals("CN") && rdn.getValue().toString().contains(requiredCommonNameString)) {
              return;
            }
            throw new CertificateException("Missing proper CN to access this cluster");
          }
        } catch (Exception e) {
          LOG.debug("Failed to authenticate", e);
          throw e;
        }
      }
    }
  }
}
