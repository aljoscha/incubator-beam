///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.beam.runners.flink;
//
//import com.google.common.collect.Iterables;
//import com.spotify.docker.client.DefaultDockerClient;
//import com.spotify.docker.client.DockerClient;
//import com.spotify.docker.client.messages.ContainerConfig;
//import com.spotify.docker.client.messages.ContainerCreation;
//import com.spotify.docker.client.messages.ContainerInfo;
//import com.spotify.docker.client.messages.HostConfig;
//import com.spotify.docker.client.messages.PortBinding;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class DockerTesting {
//
//  public static void main(String[] args) throws Exception {
//    final DockerClient docker = DefaultDockerClient.fromEnv().build();
//
//    //docker.pull("...");
//
//    final Map<String, List<PortBinding>> portBindings = new HashMap<>();
//
//    // Bind container port 80 to an automatically allocated available host port.
//    List<PortBinding> randomPort = new ArrayList<>();
//    randomPort.add(PortBinding.randomPort("0.0.0.0"));
//    portBindings.put("80", randomPort);
//
//    final HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();
//
//    // Create container with exposed ports
//    final ContainerConfig containerConfig = ContainerConfig.builder()
//        .hostConfig(hostConfig)
//        .image("testing-container").exposedPorts(new String[] {"80"})
//        .image("testing-container")
//        .build();
//
//    final ContainerCreation creation = docker.createContainer(containerConfig);
//    final String id = creation.id();
//
//    docker.startContainer(id);
//
//    final ContainerInfo info = docker.inspectContainer(id);
//
//    System.out.println("PORTS: " + info.networkSettings().ports());
//
//    PortBinding outsidePort = Iterables.getFirst(info.networkSettings().ports().get("80/tcp"), null);
//
//    if (outsidePort == null) {
//      throw new RuntimeException("Didn't get a port mapping.");
//    }
//
////    String result = Request.Get("http://localhost:" + outsidePort.hostPort() + "/aljoscha")
////        .connectTimeout(1000)
////        .socketTimeout(1000)
////        .execute().returnContent().asString();
//
////    System.out.println("RESULT: " + result);
//
//    docker.killContainer(id);
//
//    docker.removeContainer(id);
//
//    docker.close();
//  }
//}
