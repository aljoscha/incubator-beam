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
package org.apache.beam.runners.flink;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.http.client.fluent.Request;

/**
 * For this to work you need to have the "testing-container" in your local Docker repo. Build
 * it in the "testing-container" directory using "docker build -t testing-container .".
 */
public class DockerFlinkJob {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    env.setParallelism(1);

    DataStream<Long> source = env.addSource(new InfiniteLongSource());

    source
        .map(new DockerOperator())
        .flatMap(new ThroughputCounter());

    env.execute();
  }

  private static class InfiniteLongSource implements SourceFunction<Long> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
      long index = 0;
      while (isRunning) {
        ctx.collect(index++);
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }

  private static class DockerOperator extends RichMapFunction<Long, String> {
    private transient DockerClient docker;
    private transient int port;
    private transient String containerId;

    @Override
    public void open(Configuration parameters) throws Exception {
      docker = DefaultDockerClient.fromEnv().build();

      final Map<String, List<PortBinding>> portBindings = new HashMap<>();

      // Bind container port 80 to an automatically allocated available host port.
      List<PortBinding> randomPort = new ArrayList<>();
      randomPort.add(PortBinding.randomPort("0.0.0.0"));
      portBindings.put("80", randomPort);

      final HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();

      // Create container with exposed ports
      final ContainerConfig containerConfig = ContainerConfig.builder()
          .hostConfig(hostConfig)
          .image("testing-container").exposedPorts(new String[] {"80"})
          .image("testing-container")
          .build();

      final ContainerCreation creation = docker.createContainer(containerConfig);
      containerId = creation.id();

      docker.startContainer(containerId);

      final ContainerInfo info = docker.inspectContainer(containerId);

      PortBinding outsidePort = Iterables.getFirst(info.networkSettings().ports().get("80/tcp"), null);

      if (outsidePort == null) {
        throw new RuntimeException("Didn't get a port mapping.");
      }
      port = Integer.parseInt(outsidePort.hostPort());
    }

    @Override
    public void close() throws Exception {
      docker.killContainer(containerId);
      docker.removeContainer(containerId);
      docker.close();
    }

    @Override
    public String map(Long value) throws Exception {

      while (true) {
        try {
          String result = Request.Get("http://localhost:" + port + "/" + value)
              .connectTimeout(10000)
              .socketTimeout(10000)
              .execute().returnContent().asString();
          return result;
        } catch (Exception e) {
          System.err.println(e);
        }
      }
    }
  }

  private static class ThroughputCounter extends RichFlatMapFunction<String, Void> {
    private transient Stopwatch watch;
    private transient long count;

    @Override
    public void open(Configuration parameters) throws Exception {
      watch = Stopwatch.createStarted();
      count = 0;
    }

    @Override
    public void flatMap(String value, Collector<Void> out) throws Exception {
      count++;
      if (watch.elapsed(TimeUnit.MILLISECONDS) > 1000) {
        System.out.println("Elements/s " + ((float) count / watch.elapsed(TimeUnit.MILLISECONDS) * 1000));
        count = 0;
        watch.reset();
        watch.start();
      }
    }
  }
}
