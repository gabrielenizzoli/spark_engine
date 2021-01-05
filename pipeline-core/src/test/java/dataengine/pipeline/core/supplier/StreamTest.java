package dataengine.pipeline.core.supplier;

import dataengine.pipeline.core.consumer.impl.DatasetWriterFormat;
import dataengine.pipeline.core.consumer.impl.StreamConsumer;
import dataengine.pipeline.core.supplier.impl.SparkSource;
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

        StreamConsumer<Row> sparkSink = StreamConsumer.<Row>builder()
                .queryName("memoryTable")
                .format(DatasetWriterFormat.builder().format("memory").build())
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
