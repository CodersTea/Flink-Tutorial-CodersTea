package com.coderstea.bigdata.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Sink files with Flink Batch Streaming
 */
public class FlinkBatchFileSink {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting Flink batch to streamingFileSink and fileSink");
        StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        exeEnv.setRuntimeMode(RuntimeExecutionMode.BATCH); // use batch mode, !important
        exeEnv.setParallelism(1);
        // there is no need of checkpointing configuration

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.forRowFormat(
                        new Path("src/main/resources/output"), new SimpleStringEncoder<String>()
                )
                .withBucketAssigner(new BasePathBucketAssigner<>())
                .build();

        FileSink<String> fileSink = FileSink.forRowFormat(
                        new Path("src/main/resources/output1"), new SimpleStringEncoder<String>()
                )
                .withBucketAssigner(new BasePathBucketAssigner<>())
                .build();
        String filePath = FlinkBatchAsStreaming.class.getClassLoader().getResource("expense-report.csv").getPath();
        TextInputFormat txt = new TextInputFormat(new Path(filePath));

        DataStream<String> stream = exeEnv.createInput(txt)
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
                .sum(1)
                .map(x -> x.f0 + "," + x.f1);

        // Streaming streamingFileSink
        stream.addSink(streamingFileSink);
        // output: .part-0-0.inprogress.052e111b-79b1-4d2c-ac3c-6a6d63549652

        stream.sinkTo(fileSink);
        // output: part-13d796db-4956-489f-a0c3-c30baaf30b98-0

        exeEnv.execute();
    }
}