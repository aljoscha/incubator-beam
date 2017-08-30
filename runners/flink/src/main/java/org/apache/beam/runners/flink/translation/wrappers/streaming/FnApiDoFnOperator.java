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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.fn.v1.BeamFnControlGrpc;
import org.apache.beam.fn.v1.BeamFnLoggingGrpc;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.FnApiControlClient;
import org.apache.beam.runners.core.FnApiControlClientPoolService;
import org.apache.beam.runners.core.FnApiDataClientMultiplexingService;
import org.apache.beam.runners.core.SdkHarnessClient;
import org.apache.beam.runners.core.SdkHarnessDoFnRunner;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.dataflow.util.DoFnInfo;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.slf4j.Logger;

/**
 * Flink operator for executing {@link DoFn DoFns}.
 *
 * @param <InputT> the input type of the {@link DoFn}
 * @param <OutputT> the output type of the {@link DoFn}
 */
public class FnApiDoFnOperator<InputT, OutputT>
    extends DoFnOperator<InputT, OutputT> {

  private final RunnerApi.PTransform pTransform;
  private final RunnerApi.Pipeline pipeline;

  private transient Server loggingServer;
  private transient Server controlServer;
  private transient Server dataServer;
  private transient Thread fnHarnessThread;

  private transient BlockingQueue<FnApiControlClient> controlApiClientQueue;
  private transient SdkHarnessClient sdkHarnessClient;

  private transient volatile SdkHarnessClient.ActiveBundle activeBundle;

  public FnApiDoFnOperator(
      DoFn<InputT, OutputT> doFn,
      String stepName,
      Coder<WindowedValue<InputT>> inputCoder,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      DoFnOperator.OutputManagerFactory<OutputT> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<?> keyCoder,
      RunnerApi.PTransform pTransform,
      RunnerApi.Pipeline pipeline) {
    super(
        doFn,
        stepName,
        inputCoder,
        mainOutputTag,
        additionalOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options,
        keyCoder);

    this.pTransform = pTransform;
    this.pipeline = pipeline;
  }

  // Outputting to a real "Log" doesn't work as long as the FnHarness is running in the same
  // process because it's "bending the pipes"
  private static BeamFnLoggingGrpc.BeamFnLoggingImplBase createLoggingService(final Logger log) {
    return new BeamFnLoggingGrpc.BeamFnLoggingImplBase() {
      @Override
      public StreamObserver<BeamFnApi.LogEntry.List> logging(
          final StreamObserver<BeamFnApi.LogControl> responseObserver) {
        return new StreamObserver<BeamFnApi.LogEntry.List>() {
          @Override
          public void onNext(BeamFnApi.LogEntry.List value) {
            for (BeamFnApi.LogEntry entry : value.getLogEntriesList()) {
              // TODO: don't log everything as INFO
//              log.info(entry.getMessage());
              System.out.println("LOG: " + entry.getMessage());
            }
          }

          @Override
          public void onError(Throwable t) {
//            log.error("Error from SDK.", t);
            System.out.println("Error from SDK: " + t);
          }

          @Override
          public void onCompleted() {
            System.out.println("SDK completed.");
          }
        };
      }
    };
  }

  private BeamFnApi.ProcessBundleDescriptor createProcessBundleDescriptor(
      String descriptorId,
      String dataServiceUrl,
      String sourceId,
      String doFnId) {
    BeamFnApi.ApiServiceDescriptor dataDescriptor = BeamFnApi.ApiServiceDescriptor
        .newBuilder()
        .setId("1L")
        .setUrl(dataServiceUrl)
        .build();

    RunnerApi.FunctionSpec functionSpec = RunnerApi.FunctionSpec.newBuilder()
        .setUrn("urn:org.apache.beam:source:runner:0.1")
        .setPayload(BeamFnApi.RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(dataDescriptor).build().toByteString())
        .build();

    RunnerApi.PTransform rpcSourceTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(functionSpec)
        .putAllOutputs(pTransform.getInputsMap())
        .build();

    // translate the DoFn PTransform
    DoFnInfo<?, ?> doFnInfo = DoFnInfo.forFn(
        doFn,
        windowingStrategy,
        sideInputs,
        ((WindowedValue.WindowedValueCoder<InputT>) inputCoder).getValueCoder(),
        0,
        ImmutableMap.<Long, TupleTag<?>>of(0L, mainOutputTag));

    RunnerApi.FunctionSpec transformedFunctionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(ParDoTranslation.CUSTOM_JAVA_DO_FN_URN)
            .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(doFnInfo)))
            .build();

    RunnerApi.PTransform transformedPTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(transformedFunctionSpec)
        .putAllInputs(pTransform.getInputsMap())
        .putOutputs("0", "Output")
        .build();

    // find the one input of our DoFn
    String inputPCollectionId = getOnlyElement(pTransform.getInputsMap().values());

    RunnerApi.PCollection inputPCollection =
        this.pipeline.getComponents().getPcollectionsMap().get(inputPCollectionId);

    SdkComponents coderComponents = SdkComponents.create();
    String windowedValueCoderName;
    try {
      // the coder, along with all components coder will be in the components
      windowedValueCoderName = coderComponents.registerCoder(inputCoder);
    } catch (IOException e) {
      throw new RuntimeException("Error creating coder.", e);
    }

    // patch the new coder into the input collection
    RunnerApi.PCollection patchedInputPCollection = RunnerApi.PCollection.newBuilder()
        .mergeFrom(inputPCollection)
        .setCoderId(windowedValueCoderName)
        .build();

    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .setId(descriptorId)
            .putTransforms(sourceId, rpcSourceTransform)
            .putTransforms(doFnId, transformedPTransform)
            .putPcollections(inputPCollectionId, patchedInputPCollection)
            .putAllCoders(coderComponents.toComponents().getCodersMap())
            .build();

    return processBundleDescriptor;
  }

  @Override
  public void open() throws Exception {
    final PipelineOptions options = serializedOptions.get();

    BeamFnLoggingGrpc.BeamFnLoggingImplBase loggingService = createLoggingService(LOG);

    FnApiDataClientMultiplexingService dataService = FnApiDataClientMultiplexingService.create();

    controlApiClientQueue = new SynchronousQueue<>();
    BeamFnControlGrpc.BeamFnControlImplBase controlService =
        FnApiControlClientPoolService.offeringClientsToPool(controlApiClientQueue);

    dataServer = ServerBuilder.forPort(0).addService(dataService).build();
    dataServer.start();
    loggingServer = ServerBuilder.forPort(0).addService(loggingService).build();
    loggingServer.start();
    controlServer = ServerBuilder.forPort(0).addService(controlService).build();
    controlServer.start();

    final BeamFnApi.ApiServiceDescriptor loggingDescriptor = BeamFnApi.ApiServiceDescriptor
        .newBuilder()
        .setId(UUID.randomUUID().toString()) // TODO: figure out if these can have some useful value
        .setUrl("localhost:" + loggingServer.getPort())
        .build();

    final BeamFnApi.ApiServiceDescriptor controlDescriptor = BeamFnApi.ApiServiceDescriptor
        .newBuilder()
        .setId(UUID.randomUUID().toString()) // TODO: figure out if these can have some useful value
        .setUrl("localhost:" + controlServer.getPort())
        .build();

    fnHarnessThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          FnHarness.main(options, loggingDescriptor, controlDescriptor);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    fnHarnessThread.start();

    FnApiControlClient fnApiControlClient = controlApiClientQueue.take();

    sdkHarnessClient = SdkHarnessClient.usingFnApiClient(fnApiControlClient, dataService);

    // make this synchronous via .get()
    sdkHarnessClient
        .register(Collections.singleton(
            createProcessBundleDescriptor(
                "descriptor_1",
                "localhost:" + dataServer.getPort(),
                "source_id",
                "dofn_id")))
        .get();

    super.open();
  }

  @Override
  protected DoFnRunner<InputT, OutputT> createDoFnRunner(
      FlinkPipelineOptions options,
      StepContext stepContext,
      DoFn<InputT, OutputT> doFn,
      SideInputReader sideInputReader,
      BufferedOutputManager<OutputT> outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      WindowingStrategy<?, ?> windowingStrategy) {
      return SdkHarnessDoFnRunner.create(
          sdkHarnessClient,
          "descriptor_1",
          "source_id",
          getOnlyElement(pTransform.getInputsMap().keySet()),
          inputCoder);
  }

  @Override
  public void close() throws Exception {
    super.close();


    fnHarnessThread.interrupt();
    fnHarnessThread.join();

    controlServer.shutdownNow();
    dataServer.shutdownNow();
    loggingServer.shutdownNow();
  }
}
