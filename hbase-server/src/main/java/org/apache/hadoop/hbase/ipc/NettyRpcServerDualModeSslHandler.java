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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.OptionalSslHandler;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslHandler;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Future;

@InterfaceAudience.Private
public class NettyRpcServerDualModeSslHandler extends OptionalSslHandler {
  private final String requiredCommonNameString;

  public NettyRpcServerDualModeSslHandler(SslContext sslContext, String requiredCommonNameString) {
    super(sslContext);
    this.requiredCommonNameString = requiredCommonNameString;
  }

  @Override
  protected SslHandler newSslHandler(ChannelHandlerContext context, SslContext sslContext) {
    SslHandler handler = super.newSslHandler(context, sslContext);
    Future<Channel> handshakeFuture = handler.handshakeFuture();
    handshakeFuture.addListener(new NettyRpcServerSslCertificateVerifier(handler, requiredCommonNameString));
    return handler;
  }
}
