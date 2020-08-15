package dataengine.pipeline.core.source;

import dataengine.pipeline.core.sink.impl.SinkFormat;
import dataengine.pipeline.core.sink.impl.SparkStreamSink;
import dataengine.pipeline.core.source.impl.SparkSource;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StreamTest extends SparkSessionBase {

    @Test
    public void testStream() throws StreamingQueryException {

        // given - stream with source and sink
        SparkSource<Row> sparkSource = SparkSource.<Row>builder()
                .type(SparkSource.SourceType.STREAM)
                .format("rate")
                .option("rowsPerSecond ", "1")
                .build();

        SparkStreamSink<Row> sparkSink = SparkStreamSink.<Row>builder()
                .queryName("memoryTable")
                .format(SinkFormat.builder().format("memory").build())
                .trigger(Trigger.ProcessingTime(1000))
                .outputMode(OutputMode.Append())
                .build();

        sparkSource.writeTo(sparkSink);

        // when - stream is run
        sparkSession.streams().awaitAnyTermination(5000);

        // then - at least one event is recorded
        long count = sparkSession.sql("select count(*) as count from memoryTable").as(Encoders.LONG()).collectAsList().get(0);
        Assertions.assertTrue(count > 0);
    }

}
