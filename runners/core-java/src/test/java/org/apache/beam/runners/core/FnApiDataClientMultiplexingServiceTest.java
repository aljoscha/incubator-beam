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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.stub.StreamObserver;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link FnApiDataClientMultiplexingService}. */
@RunWith(JUnit4.class)
public class FnApiDataClientMultiplexingServiceTest {

  private FnApiDataClientMultiplexingService dataService =
      FnApiDataClientMultiplexingService.create();

  @Test
  public void testMultiplexing() throws Exception {
    final String instructionIdA = "instruction_a";
    final String sourceReferenceA = "transform_a";
    final String instructionIdB = "instruction_a";
    final String sourceReferenceB = "transform_b";

    final Queue<BeamFnApi.Elements> elementsQueue = new LinkedBlockingQueue<>();

    StreamObserver<BeamFnApi.Elements> elementsObserver = new StreamObserver<BeamFnApi.Elements>() {
      @Override
      public void onNext(BeamFnApi.Elements value) {
        elementsQueue.add(value);
      }

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onCompleted() {}
    };

    StreamObserver<BeamFnApi.Elements> responseObserver =
        dataService.data(elementsObserver);

    WindowedValue.ValueOnlyWindowedValueCoder<String> coder =
        WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());

    FnDataService.LogicalEndpoint logicalEndpointA =
        FnDataService.LogicalEndpoint.of(
            instructionIdA,
            BeamFnApi.Target.newBuilder()
                .setPrimitiveTransformReference(sourceReferenceA)
                .setName("DUMMY")
                .build());

    FnDataService.LogicalEndpoint logicalEndpointB =
        FnDataService.LogicalEndpoint.of(
            instructionIdB,
            BeamFnApi.Target.newBuilder()
                .setPrimitiveTransformReference(sourceReferenceB)
                .setName("DUMMY")
                .build());

    assertFalse(logicalEndpointA.equals(logicalEndpointB));

    FnDataReceiver<WindowedValue<String>> dataReceiverA = dataService.send(logicalEndpointA, coder);
    FnDataReceiver<WindowedValue<String>> dataReceiverB = dataService.send(logicalEndpointB, coder);

    dataReceiverA.accept(WindowedValue.valueInGlobalWindow("hullo"));
    BeamFnApi.Elements elements = elementsQueue.poll();
    for (BeamFnApi.Elements.Data data : elements.getDataList()) {
      assertThat(data.getInstructionReference(), is(instructionIdA));
      assertThat(data.getTarget().getPrimitiveTransformReference(), is(sourceReferenceA));
      WindowedValue<String> value =
          CoderUtils.decodeFromByteArray(coder, data.getData().toByteArray());
      assertThat(value.getValue(), is("hullo"));
    }

    dataReceiverB.accept(WindowedValue.valueInGlobalWindow("hullo"));
    elements = elementsQueue.poll();
    for (BeamFnApi.Elements.Data data : elements.getDataList()) {
      assertThat(data.getInstructionReference(), is(instructionIdB));
      assertThat(data.getTarget().getPrimitiveTransformReference(), is(sourceReferenceB));
      WindowedValue<String> value =
          CoderUtils.decodeFromByteArray(coder, data.getData().toByteArray());
      assertThat(value.getValue(), is("hullo"));
    }
  }
}
