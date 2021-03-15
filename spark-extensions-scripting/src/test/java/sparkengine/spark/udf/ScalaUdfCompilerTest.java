package sparkengine.spark.udf;

import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Test;
import sparkengine.spark.test.SparkSessionManager;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ScalaUdfCompilerTest extends SparkSessionManager {

    @Test
    void testUdfCompilationIn() {

        // given
        var udf = ScalaUdfCompiler.compile("test", "(i:Int) => i+1");
        sparkSession.udf().register(udf.getName(), udf.getUdf1(), udf.getReturnType());

        // when
        var data = sparkSession.sql("select test(1)").as(Encoders.INT()).collectAsList();

        // then
        assertEquals(List.of(2), data);
    }

}