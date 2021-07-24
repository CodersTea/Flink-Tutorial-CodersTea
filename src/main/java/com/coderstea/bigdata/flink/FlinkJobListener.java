package com.coderstea.bigdata.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Slf4j
public class FlinkJobListener {
  public static void main(String[] args) throws Exception {
    log.info("Starting the JobListener Example Code");
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    String outPut = File.createTempFile("1", "2").getParent();
    log.info("Output will be store at folder {}", outPut);

    List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    env.fromCollection(integers)
            .map(x -> x * 2) // multiply numbers by 2
            .writeAsText(outPut + "/flink-job-listener-" + UUID.randomUUID());

    log.info("Registering the JobListener");
    env.registerJobListener(new JobListener() {
      @Override
      public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
        if (throwable != null) {
          log.error("Job failed to submit", throwable);
          return;
        }
        log.info("Job submitted successfully");
        // do something
        // push notification
        // or Call an API
        // or Insert something in DB
      }

      @Override
      public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        if (throwable != null) {
          log.error("Job failed to finish ", throwable);
          return;
        }
        log.info("Job completed successfully");
        // do something
        // push notification
        // or Call an API
        // or Insert something in DB
      }
    });

    env.execute();
  }
}
