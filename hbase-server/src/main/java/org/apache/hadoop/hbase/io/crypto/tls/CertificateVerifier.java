package org.apache.hadoop.hbase.io.crypto.tls;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslHandler;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Future;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.GenericFutureListener;

public class CertificateVerifier implements GenericFutureListener<Future<Channel>> {
  private static final Logger LOG = LoggerFactory.getLogger(CertificateVerifier.class);

  private final SslHandler sslHandler;

  CertificateVerifier(SslHandler sslHandler) {
    this.sslHandler = sslHandler;
  }

  @Override
  public void operationComplete(Future<Channel> future) throws Exception {
    if (future.isSuccess()) {
      SSLEngine engine = sslHandler.engine();
      SSLSession session = engine.getSession();

      LOG.debug("Got principal for session {}", session.getPeerPrincipal());
      verifyChain((X509Certificate[]) session.getPeerCertificates());
    }
  }

  private void verifyChain(X509Certificate[] chain) throws CertificateException {
    LOG.debug("Checking {} certs", chain.length);
    for (X509Certificate cert : chain) {
      try {
        LdapName name = new LdapName(cert.getSubjectX500Principal().getName());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Parsed ldap name {}", name);
          for (Rdn rdn : name.getRdns()) {
            LOG.debug("{} -> {}", rdn.getType(), rdn.getValue());
          }
        }
      } catch (InvalidNameException e) {
        throw new CertificateException("Could not extract ldap name from cert", e);
      }
    }
  }
}
