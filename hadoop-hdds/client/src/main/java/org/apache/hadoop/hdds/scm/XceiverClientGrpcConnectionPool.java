/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceStub;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.tracing.GrpcClientInterceptor;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_MAX_DEFAULT;

/**
 * XceiverClientGrpc Connection Pool.
 */
public class XceiverClientGrpcConnectionPool {

  private static final Logger LOG =
      LoggerFactory.getLogger(XceiverClientGrpcConnectionPool.class);

  private final ConfigurationSource config;
  private final SecurityConfig secConfig;
  private final boolean enableEcRetries;
  private final double ecMaxAttempts;

  private final ClientTrustManager trustManager;
  private final Map<UUID, Connection> connections = new ConcurrentHashMap<>();

  public XceiverClientGrpcConnectionPool(ConfigurationSource config,
                                         ClientTrustManager trustManager) {
    this.config = config;
    this.secConfig = new SecurityConfig(config);
    this.trustManager = trustManager;

    this.enableEcRetries =
        config.getBoolean(OZONE_CLIENT_EC_GRPC_RETRIES_ENABLED,
            OZONE_CLIENT_EC_GRPC_RETRIES_ENABLED_DEFAULT);
    this.ecMaxAttempts = config.getInt(OZONE_CLIENT_EC_GRPC_RETRIES_MAX,
        OZONE_CLIENT_EC_GRPC_RETRIES_MAX_DEFAULT);
  }

  @VisibleForTesting
  public int getRefcount(DatanodeDetails dn) {
    Connection connection = connections.get(dn.getUuid());
    if (connection != null) {
      return connection.getRefcount();
    }
    return 0;
  }

  public synchronized Connection connect(DatanodeDetails dn)
      throws IOException {
    UUID dnUuid = dn.getUuid();
    Connection connection = connections.get(dnUuid);
    if (connections.get(dnUuid) == null) {
      connection = new Connection(dn);
      connections.put(dn.getUuid(), connection);
      connection.connect();
    }
    connection.retain();
    return connection;
  }

  public void close(UUID dnId) {
    Connection connection = connections.get(dnId);
    if (connection != null) {
      connection.release();
    }
  }

  /**
   * XceiverClientGrpc Connection Entry.
   */
  public class Connection {
    private final DatanodeDetails dn;

    private XceiverClientProtocolServiceStub asyncStub;
    private ManagedChannel channel;

    private XceiverClientProtocolServiceStub ecAsyncStub;
    private ManagedChannel ecChannel;

    private final AtomicInteger ref = new AtomicInteger(0);

    public Connection(DatanodeDetails dn) {
      this.dn = dn;
    }

    public XceiverClientProtocolServiceStub getAsyncStub() {
      return asyncStub;
    }

    public XceiverClientProtocolServiceStub getEcAsyncStub() {
      return ecAsyncStub;
    }

    public synchronized void connect() throws IOException {
      if (isConnected()) {
        return;
      }
      // read port from the data node, on failure use default configured
      // port.
      int port = dn.getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
      if (port == 0) {
        port = config.getInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
            OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT);
      }

      // Add credential context to the client call
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to server : {} ref: {}", dn.getIpAddress(), ref.get());
      }
      channel = createChannel(dn, port).build();
      asyncStub = XceiverClientProtocolServiceGrpc.newStub(channel);

      ecChannel = createEcChannel(dn, port).build();
      ecAsyncStub = XceiverClientProtocolServiceGrpc.newStub(ecChannel);
    }

    public int getRefcount() {
      return ref.get();
    }

    public void retain() {
      int r = ref.incrementAndGet();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Retain : {} ref: {}", dn.getIpAddress(), r);
      }
    }

    public synchronized void release() {
      if (ref.get() > 0) {
        int r = ref.decrementAndGet();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Release : {} ref: {}", dn.getIpAddress(), r);
        }
        if (r <= 0) {
          if (isConnected()) {
            close();
          }
        }
      }
    }

    public synchronized void checkOpen() throws IOException {
      if (!isConnected()) {
        connect();
      }
      if (!isConnected()) {
        throw new IOException("This channel is not connected.");
      }
    }

    public boolean isConnected() {
      return isChannelConnected(channel) && isChannelConnected(ecChannel);
    }

    private boolean isChannelConnected(ManagedChannel managedChannel) {
      return managedChannel != null && !managedChannel.isTerminated() &&
          !managedChannel.isShutdown();
    }

    private NettyChannelBuilder createChannel(DatanodeDetails datanode,
        int port) throws IOException {
      NettyChannelBuilder channelBuilder =
          NettyChannelBuilder.forAddress(datanode.getIpAddress(), port)
              .usePlaintext()
              .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
              .intercept(new GrpcClientInterceptor());
      if (secConfig.isSecurityEnabled() && secConfig.isGrpcTlsEnabled()) {
        SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
        if (trustManager != null) {
          sslContextBuilder.trustManager(trustManager);
        }
        if (secConfig.useTestCert()) {
          channelBuilder.overrideAuthority("localhost");
        }
        channelBuilder.useTransportSecurity()
            .sslContext(sslContextBuilder.build());
      } else {
        channelBuilder.usePlaintext();
      }
      return channelBuilder;
    }

    private NettyChannelBuilder createEcChannel(DatanodeDetails datanode,
        int port) throws IOException {
      NettyChannelBuilder channelBuilder = createChannel(datanode, port);
      if (enableEcRetries) {
        channelBuilder.defaultServiceConfig(
                createEcRetryServiceConfig(ecMaxAttempts))
            .maxRetryAttempts((int) ecMaxAttempts).enableRetry();
      }
      return channelBuilder;
    }

    private Map<String, Object> createEcRetryServiceConfig(double maxAttempts) {
      Map<String, Object> retryPolicy = new HashMap<>();
      // Maximum number of RPC attempts which includes the original RPC.
      retryPolicy.put("maxAttempts", maxAttempts);
      // The initial retry attempt will occur at random(0, initialBackoff)
      retryPolicy.put("initialBackoff", "0.5s");
      retryPolicy.put("maxBackoff", "3s");
      retryPolicy.put("backoffMultiplier", 1.5D);
      //Status codes for with RPC retry are attempted.
      retryPolicy.put("retryableStatusCodes",
          Collections.singletonList(Status.Code.DEADLINE_EXCEEDED.name()));
      Map<String, Object> methodConfig = new HashMap<>();
      methodConfig.put("retryPolicy", retryPolicy);

      Map<String, Object> name = new HashMap<>();
      name.put("service", "hadoop.hdds.datanode.XceiverClientProtocolService");
      methodConfig.put("name", Collections.singletonList(name));

      Map<String, Object> serviceConfig = new HashMap<>();
      serviceConfig.put("methodConfig",
          Collections.singletonList(methodConfig));
      return serviceConfig;
    }

    private void close() {
      if (channel != null) {
        closeChannel(channel);
      }
      if (ecChannel != null) {
        closeChannel(ecChannel);
      }
    }

    private void closeChannel(ManagedChannel managedChannel) {
      managedChannel.shutdownNow();
      try {
        managedChannel.awaitTermination(60, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.error("InterruptedException while waiting for channel termination",
            e);
        // Re-interrupt the thread while catching InterruptedException
        Thread.currentThread().interrupt();
      }
    }
  }
}
