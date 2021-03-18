package sparkengine.spark.sql.udf;

import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Test;
import sparkengine.spark.test.SparkSessionManager;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;



class ScalaUdfCompilerTest extends SparkSessionManager {

    @Test
    void testUdfCompilation() throws Exception {

        // given
        var udf = ScalaUdfCompiler.compile("test", "(i:Int) => i+1");
        sparkSession.udf().register(udf.getName(), udf.getUdf1(), udf.getReturnType());

        // when
        var data = sparkSession.sql("select test(1)").as(Encoders.INT()).collectAsList();

        // then
        assertEquals(List.of(2), data);
    }

    @Test
    void testUdfCompilationWithUDT() throws Exception {

        // given
        new TestUserDefinedType().register();
        var udf1 = ScalaUdfCompiler.compile("createBean", "import sparkengine.spark.sql.udf.TestBean; (i:Int) => { val t = new TestBean(); t.setValue(i); t }");
        var udf2 = ScalaUdfCompiler.compile("incrBean", "import sparkengine.spark.sql.udf.TestBean; (i:TestBean, j:Int) => { i.setValue(i.getValue()+j); i }");
        sparkSession.udf().register(udf1.getName(), udf1.getUdf1(), udf1.getReturnType());
        sparkSession.udf().register(udf2.getName(), udf2.getUdf2(), udf2.getReturnType());

        // when
        var rows = sparkSession.sql("select incrBean(createBean(1), 99) as xxx").collectAsList();

        // then
        assertEquals(List.of(TestBean.of(100)), rows.stream().map(row -> row.<TestBean>getAs("xxx")).collect(Collectors.toList()));
    }


    @Test
    void testUdfCompilationWithBadCode() {
        assertThrows(UdfCompilationException.class, () -> ScalaUdfCompiler.compile("test", "(i:Int) => i+ ; 1"));
    }

    @Test
    void testUdfCompilationWithBadReturnType() {
        assertThrows(UdfCompilationException.class, () -> ScalaUdfCompiler.compile("test", "1"));
    }

}