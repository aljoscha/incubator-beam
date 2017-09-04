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
package org.apache.beam.runners.core;

import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.fn.v1.BeamFnDataGrpc;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Fn API data service that multiplexes multiple logical connections (identified by a
 * {@link LogicalEndpoint}) over one physical {@link BeamFnDataGrpc} connection.
 */
public class FnApiDataClientMultiplexingService
    extends BeamFnDataGrpc.BeamFnDataImplBase
    implements FnDataService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FnApiDataClientMultiplexingService.class);

  private transient AtomicReference<StreamObserver<BeamFnApi.Elements>> dataChannel;

  private FnApiDataClientMultiplexingService() {
    this.dataChannel = new AtomicReference<>();
  }

  /**
   * Creates a new {@link FnApiDataClientMultiplexingService} which allows getting
   * {@link FnDataReceiver FnDataReceivers} for a logical channel by providing a
   * {@link LogicalEndpoint} and a {@link Coder}.
   * }
   */
  public static FnApiDataClientMultiplexingService create() {
    return new FnApiDataClientMultiplexingService();
  }

  @Override
  public StreamObserver<BeamFnApi.Elements> data(
      StreamObserver<BeamFnApi.Elements> responseObserver) {
    LOGGER.info("Beam Fn Data client connected.");

    this.dataChannel.set(responseObserver);

    // TODO properly patch this in somewhere
    return new StreamObserver<BeamFnApi.Elements>() {
      @Override
      public void onNext(BeamFnApi.Elements value) {
        System.out.println("DATA RETURN: " + value);

      }

      @Override
      public void onError(Throwable t) {
        System.out.println("DATA ERROR: " + t);

      }

      @Override
      public void onCompleted() {
        System.out.println("DATA COMPLETED");
      }
    };
  }

  @Override
  public <T> ListenableFuture<Void> listen(
      LogicalEndpoint inputLocation,
      Coder<WindowedValue<T>> coder,
      FnDataReceiver<WindowedValue<T>> listener) throws Exception {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public <T> FnDataReceiver<WindowedValue<T>> send(
      final LogicalEndpoint outputLocation, final Coder<WindowedValue<T>> coder) throws Exception {

    return new FnDataReceiver<WindowedValue<T>>() {
      @Override
      public void accept(WindowedValue<T> input) throws Exception {
        while (dataChannel.get() == null) {
          Thread.sleep(10);
        }

        BeamFnApi.Elements elements = BeamFnApi.Elements.newBuilder()
            .addData(BeamFnApi.Elements.Data.newBuilder()
                .setInstructionReference(outputLocation.getInstructionId())
                .setTarget(outputLocation.getTarget())
                .setData(ByteString.copyFrom(encodeToByteArray(coder, input))))
            .build();
        dataChannel.get().onNext(elements);
      }

      @Override
      public void close() throws IOException {
        // a Bundle is closed by sending an "Elements" with empty Data
        BeamFnApi.Elements elements = BeamFnApi.Elements.newBuilder()
            .addData(BeamFnApi.Elements.Data.newBuilder()
                .setInstructionReference(outputLocation.getInstructionId())
                .setTarget(outputLocation.getTarget()))
            .build();
        dataChannel.get().onNext(elements);
      }
    };
  }
}
