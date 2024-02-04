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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import java.util.concurrent.TimeUnit;

/**
 * {@link XceiverClientSpi} implementation to work specifically with EC
 * related requests. The only difference at the moment from the basic
 * {@link XceiverClientGrpc} is that this implementation does async calls when
 * a write request is posted via the sendCommandAsync method.
 *
 * @see <a href="https://issues.apache.org/jira/browse/HDDS-5954">HDDS-5954</a>
 */
public class ECXceiverClientGrpc extends XceiverClientGrpc {

  public ECXceiverClientGrpc(
      Pipeline pipeline,
      ConfigurationSource config, XceiverClientGrpcConnectionPool pool) {
    super(pipeline, config, pool);
    setTimeout(config.getTimeDuration(OzoneConfigKeys.
        OZONE_CLIENT_EC_GRPC_WRITE_TIMEOUT, OzoneConfigKeys
        .OZONE_CLIENT_EC_GRPC_WRITE_TIMEOUT_DEFAULT, TimeUnit.SECONDS));
  }

  /**
   * For EC writes, due to outside syncronization points during writes, it is
   * not necessary to block any async requests that are
   * arriving via the
   * {@link #sendCommandAsync(ContainerProtos.ContainerCommandRequestProto)}
   * method.
   *
   * @param request the request we need the decision about
   * @return false always to do not block async requests.
   */
  @Override
  protected boolean shouldBlockAndWaitAsyncReply(
      ContainerProtos.ContainerCommandRequestProto request) {
    return false;
  }
}
