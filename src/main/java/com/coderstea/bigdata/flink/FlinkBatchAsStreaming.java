package com.coderstea.bigdata.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkBatchAsStreaming {
  public static void main(String[] args) throws Exception {
    System.out.println("Starting Flink batch as Streaming");
    StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    exeEnv.setRuntimeMode(RuntimeExecutionMode.BATCH); // use batch mode, !important
    // there is no need of checkpointing configuration

    String filePath = FlinkBatchAsStreaming.class.getClassLoader().getResource("expense-report.csv").getPath();
    TextInputFormat txt = new TextInputFormat(new Path(filePath));

    exeEnv.createInput(txt)
            .filter(x -> !x.isEmpty())
            // split the row with comma and create a tuple
            .map(new MapFunction<String, Tuple2<String, Integer>>() {
              @Override
              public Tuple2<String, Integer> map(String row) throws Exception {
                // split category and amount
                String[] categoryAndAmount = row.split(",");
                return new Tuple2<>(categoryAndAmount[0], Integer.parseInt(categoryAndAmount[1]));
              }
            })
            .keyBy(t -> t.f0)// group by category which is 0th field
            // can be written as keyBy(0) but its deprecated
            .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
            // window is required. You can adjust it based on your data
            .sum(1) // sum the amount which is on 1st position
            .print();
    exeEnv.execute();
  }
}

