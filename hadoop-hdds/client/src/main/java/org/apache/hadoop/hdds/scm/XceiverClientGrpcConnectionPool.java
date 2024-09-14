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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_POOL_CONNECTION_PRE_DN;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_POOL_CONNECTION_PRE_DN_DEFAULT;

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
  private final int connectPreDn;

  private final ClientTrustManager trustManager;
  private final Map<UUID, Connections> connections = new ConcurrentHashMap<>();
  private final Map<UUID, Connections> ecConnections = new ConcurrentHashMap<>();

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
    this.connectPreDn = config.getInt(OZONE_CLIENT_POOL_CONNECTION_PRE_DN,
        OZONE_CLIENT_POOL_CONNECTION_PRE_DN_DEFAULT);
  }

  public Connection connect(DatanodeDetails dn) throws IOException {
    return connectInternal(dn, connections, false);
  }

  public Connection connectEc(DatanodeDetails dn) throws IOException {
    return connectInternal(dn, ecConnections, true);
  }

  private Connection connectInternal(DatanodeDetails dn,
      Map<UUID, Connections> connectionsMap, boolean isEc) throws IOException {
    UUID dnUuid = dn.getUuid();
    Connections connectionPool = connectionsMap.get(dnUuid);
    Connection connection = null;
    if (connectionPool == null) {
      synchronized (this) {
        if (connectionsMap.get(dnUuid) == null) {
          connectionPool = connectionsMap.get(dnUuid);
          if (connectionPool == null) { // double check
            connectionPool = new Connections(connectPreDn, dn, isEc);
            connectionsMap.put(dnUuid, connectionPool);
            connection = connectionPool.getConnection();
          }
        }
      }
    } else {
      connection = connectionPool.getConnection();
    }
    if (connection != null) {
      connection.connectIfNeed();
      connection.retain();
    }
    return connection;
  }

  public void close(Connection connection) {
    connection.release();
  }

  /**
   * Connection Entry pool per datanode.
   */
  public class Connections {
    private final List<Connection> connections = new ArrayList<>();
    private final int connects;
    private int index = 0;
    private final boolean isEc;
    private final DatanodeDetails dn;
    private int port;

    public Connections(int connects, DatanodeDetails dn, boolean isEc) {
      this.connects = connects;
      this.isEc = isEc;
      this.dn = dn;
      // read port from the data node, on failure use default configured
      // port.
      port = dn.getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
      if (port == 0) {
        port = config.getInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
            OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT_DEFAULT);
      }
    }

    public synchronized Connection getConnection() {
      if (connects > connections.size()) {
        Connection connection;
        if (isEc) {
          connection = new Connection(dn, port);
        } else {
          connection = new EcConnection(dn, port);
        }
        connections.add(connection);
        nextIndex();
        return connection;
      } else {
        return connections.get(nextIndex());
      }
    }

    private int nextIndex() {
      if ((index + 1) >= Integer.MAX_VALUE) {
        index = 0;
      }
      index++;
      return index % connects;
    }
  }

  /**
   * XceiverClientGrpc Connection Entry.
   */
  public class Connection {
    private final DatanodeDetails dn;
    private final int port;

    private XceiverClientProtocolServiceStub asyncStub;
    private ManagedChannel channel;

    private final AtomicInteger ref = new AtomicInteger(0);

    public Connection(DatanodeDetails dn, int port) {
      this.dn = dn;
      this.port = port;
    }

    public XceiverClientProtocolServiceStub getAsyncStub() {
      return asyncStub;
    }

    public synchronized void connectIfNeed() throws IOException {
      if (!isChannelConnected(channel)) {
        // Add credential context to the client call
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to server : {} ref: {}", dn.getIpAddress(),
              ref.get());
        }
        channel = createChannel(dn).build();
        asyncStub = XceiverClientProtocolServiceGrpc.newStub(channel);
      }
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
          close();
        }
      }
    }

    public boolean isConnected() {
      return isChannelConnected(channel);
    }

    private boolean isChannelConnected(ManagedChannel managedChannel) {
      return managedChannel != null && !managedChannel.isTerminated() &&
          !managedChannel.isShutdown();
    }

    protected NettyChannelBuilder createChannel(DatanodeDetails datanode)
        throws IOException {
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

    private void close() {
      closeChannel(channel);
    }

    private void closeChannel(ManagedChannel managedChannel) {
      if (isChannelConnected(managedChannel)) {
        managedChannel.shutdownNow();
        try {
          managedChannel.awaitTermination(60, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
          LOG.error(
              "InterruptedException while waiting for channel termination", e);
          // Re-interrupt the thread while catching InterruptedException
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * ECXceiverClientGrpc Connection Entry.
   */
  public class EcConnection extends Connection {
    public EcConnection(DatanodeDetails dn, int port) {
      super(dn, port);
    }

    @Override
    protected NettyChannelBuilder createChannel(DatanodeDetails datanode)
        throws IOException {
      NettyChannelBuilder channelBuilder = super.createChannel(datanode);
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
  }

}
