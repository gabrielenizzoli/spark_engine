package sparkengine.spark.transformation;

import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Test;
import sparkengine.spark.test.SparkSessionManager;
import sparkengine.spark.transformation.context.DefaultDataTransformationContext;
import sparkengine.spark.utils.SparkUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ScalaScriptMapFunctionTest extends SparkSessionManager {

    @Test
    void testMapWithScalaScriptAndContext() {

        // given
        var strings = List.of("a", "ab", "abc", "a");
        var df = sparkSession.createDataset(strings, Encoders.STRING());
        var ctx = DefaultDataTransformationContext.builder()
                .fallbackAccumulator(SparkUtils.longAnonymousAccumulator(sparkSession))
                .build();
        var mapper = new ScalaScriptMapFunction<String, Integer>("(i:String) => { ctx.acc(\"hi\"); i.length() }", SparkUtils.broadcast(sparkSession, ctx));

        // when
        var output = df.map(mapper, Encoders.INT()).collectAsList();

        // then
        assertEquals(strings.stream().map(String::length).collect(Collectors.toList()), output);
        assertEquals(ctx.getFallbackAccumulator().value(), output.size());

    }

    @Test
    void testMapWithScalaScriptAndNoContext() {

        // given
        var strings = List.of("a", "ab", "abc", "a");
        var df = sparkSession.createDataset(strings, Encoders.STRING());
        var mapper = new ScalaScriptMapFunction<String, Integer>("(i:String) => { ctx.acc(\"hi\"); i.length() }", null);

        // when
        var output = df.map(mapper, Encoders.INT()).collectAsList();

        // then
        assertEquals(strings.stream().map(String::length).collect(Collectors.toList()), output);

    }

}