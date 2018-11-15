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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.google.api.services.dataflow.model.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.*;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.control.*;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.util.FastNanoClockAndSleeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Integration tests for {@link BatchDataflowWorker}. */
@RunWith(JUnit4.class)
public class BatchDataflowWorkerIntegrationTest implements Serializable {
  @Rule public transient ResetDateTimeProvider resetDateTimeProvider = new ResetDateTimeProvider();
  //@Rule public EmbeddedSdkHarness harness = new EmbeddedSdkHarness.create();

  private static class WorkerException extends Exception {}

  @Rule public FastNanoClockAndSleeper clockAndSleeper = new FastNanoClockAndSleeper();

  @Mock transient WorkUnitClient mockWorkUnitClient;

  @Mock transient DataflowWorkProgressUpdater mockProgressUpdater;

  @Mock transient DataflowWorkExecutor mockWorkExecutor;

  private transient GrpcFnServer<FnApiControlClientPoolService> controlServer;
  private transient GrpcFnServer<GrpcDataService> dataServer;
  private transient GrpcFnServer<GrpcStateService> stateServer;
  private transient GrpcFnServer<GrpcLoggingService> loggingServer;
  private transient GrpcStateService stateDelegator;

  private transient ExecutorService serverExecutor;
  private transient ExecutorService sdkHarnessExecutor;
  private transient Future<?> sdkHarnessExecutorFuture;
  private transient ExecutorService runnerHarnessExecutor;
  private transient Future<?> runnerHarnessExecutorFuture;

  private transient DataflowWorkerHarnessOptions options;
  private transient SdkHarnessRegistry sdkHarnessRegistry;

  private ThreadFactory threadFactory;

  private static class TestPortSupplier implements java.util.function.Supplier<Integer> {
    private Integer port = 12356;

    @Override
    public Integer get() {
      return port++;
    }
  }

  @Before
  public void setup() throws Exception {

    //sdkHarnessExecutorFuture.get();
  }

  @After
  public void tearDown() throws Exception {
    controlServer.close();
    stateServer.close();
    dataServer.close();
    loggingServer.close();
    sdkHarnessExecutor.shutdownNow();
    runnerHarnessExecutor.shutdownNow();
    serverExecutor.shutdownNow();
    try {
      sdkHarnessExecutorFuture.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuntimeException
          && e.getCause().getCause() instanceof InterruptedException) {
        // expected
      } else {
        throw e;
      }
    }
  }

  private static class CreateStrings extends DoFn<byte[], String> {
    @ProcessElement
    public void process(ProcessContext ctxt) {
      ctxt.output("zero");
      ctxt.output("one");
      ctxt.output("two");
    }
  }

  private static class StringLength extends DoFn<String, Long> {
    @ProcessElement
    public void process(ProcessContext ctxt) {
      ctxt.output((long) ctxt.element().length());
    }
  }

  @Test
  public void testExecution() throws Exception {
    Pipeline p = Pipeline.create();
    p.apply("impulse", Impulse.create())
        .apply("create", ParDo.of(new CreateStrings()))
        .apply("len", ParDo.of(new StringLength()))
        .apply("addKeys", WithKeys.of("foo"))
        // Use some unknown coders
        .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()))
        // Force the output to be materialized
        .apply("gbk", GroupByKey.create());
    //.apply(TextIO.write().to("lengths"));

    MockitoAnnotations.initMocks(this);
    options =
        PipelineOptionsFactory.fromArgs(
                "--experiments=beam_fn_api", "--workerId=myWorker", "--jobId=myJob")
            .as(DataflowWorkerHarnessOptions.class);

    // Setup execution-time servers
    threadFactory = new ThreadFactoryBuilder().setDaemon(true).build();

    Endpoints.ApiServiceDescriptor.Builder loggingServerDescriptorBuilder =
        Endpoints.ApiServiceDescriptor.newBuilder();
    loggingServerDescriptorBuilder.setUrl("localhost:" + (new Random().nextInt(50000) + 10000));

    Endpoints.ApiServiceDescriptor.Builder controlServerDescriptorBuilder =
        Endpoints.ApiServiceDescriptor.newBuilder();
    controlServerDescriptorBuilder.setUrl("localhost:" + (new Random().nextInt(50000) + 10000));

    Endpoints.ApiServiceDescriptor loggingServerDescriptor = loggingServerDescriptorBuilder.build();
    Endpoints.ApiServiceDescriptor controlServerDescriptor = controlServerDescriptorBuilder.build();

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);

    runnerHarnessExecutor = Executors.newSingleThreadExecutor(threadFactory);
    runnerHarnessExecutorFuture =
        runnerHarnessExecutor.submit(
            () -> {
              try {
                DataflowRunnerHarness.main(
                    pipelineProto,
                    options,
                    loggingServerDescriptor,
                    controlServerDescriptor,
                    mockWorkUnitClient);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    Thread.sleep(5000);

    // Create the SDK harness, and wait until it connects
    sdkHarnessExecutor = Executors.newSingleThreadExecutor(threadFactory);
    sdkHarnessExecutorFuture =
        sdkHarnessExecutor.submit(
            () -> {
              try {
                FnHarness.main(
                    "id",
                    options,
                    loggingServerDescriptor,
                    controlServerDescriptor,
                    ManagedChannelFactory.createDefault(),
                    OutboundObserverFactory.clientDirect());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    runnerHarnessExecutorFuture.get();
    sdkHarnessExecutorFuture.get();
  }
}
