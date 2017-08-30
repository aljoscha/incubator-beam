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
package org.apache.beam.examples;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

 */
public class Scratch {

  private static final Logger LOG = LoggerFactory.getLogger(Scratch.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

    Pipeline p = Pipeline.create(options);

    p.apply(GenerateSequence.from(0).withRate(10, Duration.standardSeconds(1)))
//    p.apply(Create.of("Hello", "ciuao"))
        .apply(ParDo.of(new DoFn<Long, Void>() {

          private transient Stopwatch watch;
          private transient long count = 0;

          @Setup
          public void setup() {
            watch = Stopwatch.createStarted();
            count = 0;
            LOG.info("Setup called");
          }

          @StartBundle
          public void startBundle() {
//            LOG.info("Start Bundle {}", Joiner.on("\n").join(new RuntimeException().getStackTrace()));
            LOG.info("Start Bundle");
          }

          @FinishBundle
          public void finishBundle() {
//            LOG.info("Finish Bundle {}", Joiner.on("\n").join(new RuntimeException().getStackTrace()));
            LOG.info("Finish Bundle");
          }

          @ProcessElement
          public void process(ProcessContext c) {
            if (watch == null) {
              watch = Stopwatch.createStarted();
              count = 0;
            }
            count++;
//         LOG.info("GOT: {}", c.element());
            long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
            if (elapsed > 1000) {
           LOG.info("E/S {}", ((float) count / elapsed) * 1000);
//              System.out.println("E/S " + ((float) count / elapsed) * 1000);
              watch.reset();
              watch.start();
              count = 0;
            }
          }
        }));

    p.run().waitUntilFinish();
  }
}
