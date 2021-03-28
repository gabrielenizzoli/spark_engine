package sparkengine.spark.transformation;

import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Test;
import sparkengine.spark.test.SparkSessionManager;
import sparkengine.spark.transformation.context.DefaultDataTransformationContext;
import sparkengine.spark.utils.SparkUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ScalaScriptTransformationTest extends SparkSessionManager {

    @Test
    void testTransformation() {

        var strings = List.of("a", "ab", "abc", "a");
        var df = sparkSession.createDataset(strings, Encoders.STRING());
        var ctx = DefaultDataTransformationContext.builder()
                .fallbackAccumulator(SparkUtils.longAnonymousAccumulator(sparkSession))
                .build();
        var params = new ScalaScriptParameters();
        params.setCode("(i:String) => { ctx.acc(\"hi\"); i.length() }");

        var tx = new ScalaScriptTransformation<String, Integer>();
        tx.setParameters(params);
        tx.setEncoder(Encoders.INT());
        tx.setTransformationContext(SparkUtils.broadcast(sparkSession, ctx));

        // when
        var output = tx.apply(df).collectAsList();

        // then
        assertEquals(strings.stream().map(String::length).collect(Collectors.toList()), output);
        assertEquals(ctx.getFallbackAccumulator().value(), strings.size());

    }


}