package sparkengine.spark.transformation;

import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Test;
import sparkengine.spark.test.SparkSessionManager;
import sparkengine.spark.transformation.context.DefaultDataTransformationContext;
import sparkengine.spark.utils.SparkUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class ScalaScriptMapPartitionFunctionTest extends SparkSessionManager {

    @Test
    void testMapPartitionWithScalaScriptAndContext() {

        // given
        var strings = List.of("a", "ab", "abc", "a");
        var df = sparkSession.createDataset(strings, Encoders.STRING());
        var ctx = DefaultDataTransformationContext.builder()
                .fallbackAccumulator(SparkUtils.longAnonymousAccumulator(sparkSession))
                .build();
        var mapper = new ScalaScriptMapPartitionFunction<String, Integer>("(i:Iterator[String]) => { ctx.acc(\"hi\"); i.map(_.length()) }", SparkUtils.broadcast(sparkSession, ctx));

        // when
        var output = df.mapPartitions(mapper, Encoders.INT()).collectAsList();

        // then
        assertEquals(strings.stream().map(String::length).collect(Collectors.toList()), output);
        assertEquals(ctx.getFallbackAccumulator().value(), 1);

    }

    @Test
    void testMapPartitionWithScalaScriptAndNoContext() {

        // given
        var strings = List.of("a", "ab", "abc", "a");
        var df = sparkSession.createDataset(strings, Encoders.STRING());
        var mapper = new ScalaScriptMapPartitionFunction<String, Integer>("(i:Iterator[String]) => { ctx.acc(\"hi\"); i.map(_.length()) }", null);
        var ctx = DefaultDataTransformationContext.builder()
                .fallbackAccumulator(SparkUtils.longAnonymousAccumulator(sparkSession))
                .build();

        // when
        var output = df.mapPartitions(mapper, Encoders.INT()).collectAsList();

        // then
        assertEquals(strings.stream().map(s -> s.length()).collect(Collectors.toList()), output);

    }

}