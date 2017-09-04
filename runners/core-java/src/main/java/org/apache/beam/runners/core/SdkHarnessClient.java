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

import com.google.auto.value.AutoValue;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A high-level client for an SDK harness.
 *
 * <p>This provides a Java-friendly wrapper around {@link FnApiControlClient} and {@link
 * FnDataReceiver}, which handle lower-level gRPC message wrangling.
 */
public class SdkHarnessClient {

  /**
   * A supply of unique identifiers, used internally. These must be unique across all Fn API
   * clients.
   */
  public interface IdGenerator {
    String getId();
  }

  /** A supply of unique identifiers that are simply incrementing longs. */
  private static class CountingIdGenerator implements IdGenerator {
    private final AtomicLong nextId = new AtomicLong(0L);

    @Override
    public String getId() {
      return String.valueOf(nextId.incrementAndGet());
    }
  }

  /**
   * An active bundle for a particular {@link
   * org.apache.beam.fn.v1.BeamFnApi.ProcessBundleDescriptor}.
   */
  @AutoValue
  public abstract static class ActiveBundle<InputT> {
    public abstract String getBundleId();

    public abstract Future<BeamFnApi.InstructionResponse> getBundleResponse();

    public abstract FnDataReceiver<InputT> getInputReceiver();

    public static <InputT> ActiveBundle<InputT> create(
        String bundleId,
        Future<BeamFnApi.InstructionResponse> response,
        FnDataReceiver<InputT> dataReceiver) {
      return new AutoValue_SdkHarnessClient_ActiveBundle(bundleId, response, dataReceiver);
    }
  }

  private final IdGenerator idGenerator;
  private final FnApiControlClient fnApiControlClient;
  private final FnDataService dataService;

  private SdkHarnessClient(
      FnApiControlClient fnApiControlClient,
      FnDataService dataService,
      IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
    this.fnApiControlClient = fnApiControlClient;
    this.dataService = dataService;
  }

  /**
   * Creates a client for a particular SDK harness. It is the responsibility of the caller to ensure
   * that these correspond to the same SDK harness, so control plane and data plane messages can be
   * correctly associated.
   */
  public static SdkHarnessClient usingFnApiClient(
      FnApiControlClient fnApiControlClient,
      FnDataService dataService) {
    return new SdkHarnessClient(fnApiControlClient, dataService, new CountingIdGenerator());
  }

  public SdkHarnessClient withIdGenerator(IdGenerator idGenerator) {
    return new SdkHarnessClient(fnApiControlClient, dataService, idGenerator);
  }

  /**
   * Registers a {@link org.apache.beam.fn.v1.BeamFnApi.ProcessBundleDescriptor} for future
   * processing.
   *
   * <p>A client may block on the result future, but may also proceed without blocking.
   */
  public Future<BeamFnApi.RegisterResponse> register(
      Iterable<BeamFnApi.ProcessBundleDescriptor> processBundleDescriptors) {

    // TODO: validate that all the necessary data endpoints are known

    ListenableFuture<BeamFnApi.InstructionResponse> genericResponse =
        fnApiControlClient.handle(
            BeamFnApi.InstructionRequest.newBuilder()
                .setInstructionId(idGenerator.getId())
                .setRegister(
                    BeamFnApi.RegisterRequest.newBuilder()
                        .addAllProcessBundleDescriptor(processBundleDescriptors)
                        .build())
                .build());

    return Futures.transform(
        genericResponse,
        new Function<BeamFnApi.InstructionResponse, BeamFnApi.RegisterResponse>() {
          @Override
          public BeamFnApi.RegisterResponse apply(BeamFnApi.InstructionResponse input) {
            return input.getRegister();
          }
        });
  }

  /**
   * Start a new bundle for the given {@link
   * org.apache.beam.fn.v1.BeamFnApi.ProcessBundleDescriptor} identifier.
   *
   * <p>The input channels for the returned {@link ActiveBundle} are derived from the
   * instructions in the {@link org.apache.beam.fn.v1.BeamFnApi.ProcessBundleDescriptor}.
   */
  public <T> ActiveBundle newBundle(
      String processBundleDescriptorId,
      String sourceReference,
      String sourceName,
      Coder<WindowedValue<T>> coder) {
    String bundleId = idGenerator.getId();

    FnDataService.LogicalEndpoint endpoint = FnDataService.LogicalEndpoint.of(
        bundleId,
        BeamFnApi.Target.newBuilder()
            .setPrimitiveTransformReference(sourceReference)
            .setName(sourceName)
            .build());

    FnDataReceiver dataReceiver;
    try {
      dataReceiver = dataService.send(endpoint, (Coder) coder);
    } catch (Exception e) {
      throw new RuntimeException("Error creating data client.", e);
    }

    ListenableFuture<BeamFnApi.InstructionResponse> genericResponse =
        fnApiControlClient.handle(
            BeamFnApi.InstructionRequest.newBuilder()
                .setInstructionId(bundleId)
                .setProcessBundle(
                    BeamFnApi.ProcessBundleRequest.newBuilder()
                        .setProcessBundleDescriptorReference(processBundleDescriptorId))
                .build());

    return ActiveBundle.create(bundleId, genericResponse, dataReceiver);
  }
}
