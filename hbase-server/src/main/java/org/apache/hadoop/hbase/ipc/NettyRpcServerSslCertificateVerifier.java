/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ipc;

import static org.apache.hadoop.hbase.ipc.NettyRpcServer.CONNECTION_ATTRIBUTE;
import java.security.Principal;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
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
      SSLEngine engine = sslHandler.engine();
      SSLSession session = engine.getSession();
      if (engine.getNeedClientAuth() || engine.getWantClientAuth()) {
      LOG.debug("Successful handshake, trying to authenticate");
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

          LOG.debug("Got principal of typefor channel {}: {}", future.getNow().id(), principal);
          LdapName name = new LdapName(principal.getName());
          for (Rdn rdn : name.getRdns()) {
            if (rdn.getType().equals("CN")) {
              if (rdn.getValue().toString().contains(requiredCommonNameString)) {
                // another thing we could do here is pull the NettyServerRpcConnection from
                // CONNECTION_ATTRIBUTE and set some additional auth info, like conn.setAuthInfo(..)
                // with additional info pulled from the cert.  That auth info could be used to verify
                // usernames passed in ConnectionHeaders, etc.
                return;
              }
            }
          }
          sendErrorAndClose(future, "Missing proper CN to access this cluster");
        } catch (Exception e) {
          sendErrorAndClose(future, "Failed to authenticate: " + e.getMessage());
          LOG.debug("Failed to authenticate", e);
        }
      }
    } else {
      LOG.debug("Got non-success future after handshake", future.cause());
    }
  }

  private void sendErrorAndClose(Future<Channel> future, String errorMessage) {
    Channel channel = future.getNow();
    LOG.debug("Returning error to channel {}: {}", channel.id(), errorMessage);
    NettyServerRpcConnection connection = channel.attr(CONNECTION_ATTRIBUTE).get();
    NettyServerCall authError = connection.createCall(0, connection.service, null,
      null, null, null, 0, connection.addr, 0, null);
    authError.setResponse(null, null, new FatalConnectionException(errorMessage), errorMessage);
    channel.writeAndFlush(authError).addListener(ChannelFutureListener.CLOSE);
  }
}
