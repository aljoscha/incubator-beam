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
package org.apache.beam.runners.reference;

import static org.apache.beam.runners.core.construction.PipelineResources.detectClassPathResourcesToStage;

import com.google.common.collect.ImmutableList;
import io.grpc.ManagedChannel;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.core.construction.ArtifactServiceStager;
import org.apache.beam.runners.core.construction.JavaReadViaImpulse;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.util.ZipFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} a {@link Pipeline} using a {@code JobService}.
 */
public class PortableRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(PortableRunner.class);

  /**
   * Provided options.
   */
  private final PipelineOptions options;

  /**
   * Construct a runner from the provided options.
   *
   * @param options Properties which configure the runner.
   * @return The newly created runner.
   */
  public static PortableRunner fromOptions(PipelineOptions options) {

    if (options.getFilesToStage() == null) {
      options.setFilesToStage(detectClassPathResourcesToStage(
          PortableRunner.class.getClassLoader()));
      if (options.getFilesToStage().isEmpty()) {
        throw new IllegalArgumentException("No files to stage has been found.");
      } else {
        LOG.info("PipelineOptions.filesToStage was not specified. "
                + "Defaulting to files from the classpath: will stage {} files. "
                + "Enable logging at DEBUG level to see which files will be staged.",
            options.getFilesToStage().size());
      }
    }

    return new PortableRunner(options);
  }

  private PortableRunner(PipelineOptions options) {
    this.options = options;
  }

  private List<String> replaceDirectoriesWithZipFiles(List<String> paths) throws IOException {
    List<String> results = new ArrayList<>();
    for (String path : paths) {
      File file = new File(path);
      if (file.exists()) {
        if (file.isDirectory()) {
          File zipFile = File.createTempFile(file.getName(), ".zip");
          try (FileOutputStream fos = new FileOutputStream(zipFile)) {
            ZipFiles.zipDirectory(file, fos);
          }
          results.add(zipFile.getAbsolutePath());
        } else {
          results.add(path);
        }
      }
    }
    return results;
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    replaceTransforms(pipeline, false);

    LOG.info("Initial files to stage: " + options.getFilesToStage());

    // Use a hashset to deduplicate same file names.
    // TODO: Migrate to using unique names for each resource if the resource names are duplicated
    // but in different paths.
    List<String> filesToStage = new ArrayList<>();
    Set<String> filesToStageSet = new HashSet<>();
    for (String file : options.getFilesToStage()) {
      if (filesToStageSet.add(new File(file).getName())) {
        filesToStage.add(file);
      }
    }

    // TODO: Migrate this logic else where.
    try {
      filesToStage = replaceDirectoriesWithZipFiles(filesToStage);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }


    PrepareJobRequest prepareJobRequest = PrepareJobRequest.newBuilder()
        .setJobName(options.getJobName())
        .setPipeline(PipelineTranslation.toProto(pipeline))
        .setPipelineOptions(PipelineOptionsTranslation.toProto(options))
        .build();

    ManagedChannelFactory channelFactory = getChannelFactory(options);
    ManagedChannel channel = channelFactory
        .forDescriptor(getApiServiceDescriptor("localhost:3000"));

    JobServiceBlockingStub jobService = JobServiceGrpc.newBlockingStub(channel);

    PrepareJobResponse prepareJobResponse = jobService.prepare(prepareJobRequest);

    System.out.println("RESPONSE: " + prepareJobResponse);

    ApiServiceDescriptor artifactStagingEndpoint =
        prepareJobResponse.getArtifactStagingEndpoint();

    ArtifactServiceStager stager =
        ArtifactServiceStager.overChannel(channelFactory.forDescriptor(artifactStagingEndpoint));

    String stagingToken;
    try {
      LOG.info("Actual files staged {}", filesToStage);
      stagingToken =
          stager.stage(filesToStage.stream().map(File::new).collect(Collectors.toList()));
    } catch (IOException e) {
      throw new RuntimeException("Error staging files.", e);
    } catch (InterruptedException e) {
      throw new RuntimeException("Error staging files.", e);
    }

    RunJobRequest runJobRequest = RunJobRequest.newBuilder()
        .setPreparationId(prepareJobResponse.getPreparationId())
        .setStagingToken(stagingToken)
        .build();

    RunJobResponse runJobResponse = jobService.run(runJobRequest);

    System.out.println("RUN JOB RESPONSE: " + runJobResponse);

    return new JobServicePipelineResult();
  }

  private static Endpoints.ApiServiceDescriptor getApiServiceDescriptor(String descriptor) {
    Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptorBuilder =
        Endpoints.ApiServiceDescriptor.newBuilder();
    apiServiceDescriptorBuilder.setUrl(descriptor);
    return apiServiceDescriptorBuilder.build();
  }

  private ManagedChannelFactory getChannelFactory(PipelineOptions options) {
    ManagedChannelFactory channelFactory;
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    if (experiments != null && experiments.contains("beam_fn_api_epoll")) {
      channelFactory = ManagedChannelFactory.createEpoll();
    } else {
      channelFactory = ManagedChannelFactory.createDefault();
    }
    return channelFactory;
  }

  protected void replaceTransforms(Pipeline pipeline, boolean streaming) {
    pipeline.replaceAll(getOverrides(streaming));
  }

  private List<PTransformOverride> getOverrides(boolean streaming) {
    return ImmutableList.of(
        JavaReadViaImpulse.boundedOverride());
  }

  @Override
  public String toString() {
    return "PortableRunner#" + hashCode();
  }

}
