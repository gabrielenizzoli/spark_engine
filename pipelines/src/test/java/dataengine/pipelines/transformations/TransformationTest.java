package dataengine.pipelines.transformations;

import dataengine.pipelines.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

class TransformationTest extends SparkSessionTest {

    @Test
    void testSql() {

        List<Integer> data = new LinkedList<>();

        DataSource<Integer> dataSource = () -> sparkSession.createDataset(Arrays.asList(1, 2, 3, 4, 5, 6), Encoders.INT());
        DataSink<BigDecimal> dataSink = d -> data.addAll(d.collectAsList().stream().map(BigDecimal::intValue).collect(Collectors.toList()));

        DataTransformation<Integer, Row> tx1 = Transformation.sql("source", "select value*2 as value from source");
        DataTransformation<Row, Row> tx2 = Transformation.sql("source2", "select sum(value) as sumValue from source2");

        DataPipe.read(dataSource)
                .transformation(tx1)
                .transformation(tx2)
                .encode(Encoders.DECIMAL())
                .write(dataSink);

        Assertions.assertEquals(Collections.singletonList(42), data);

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor(staticName = "of")
    public static class TestBean {
        int value;
        String reference;
    }

    @Test
    void testSqlMerge() {

        // given
        List<TestBean> data = new LinkedList<>();

        DataSource<Integer> dataSource1 = () -> sparkSession.createDataset(Arrays.asList(1, 2, 3, 4, 5, 6), Encoders.INT());
        DataSource<TestBean> dataSource2 = () -> sparkSession.createDataset(Arrays.asList(TestBean.of(1, "one"), TestBean.of(6, "six")), Encoders.bean(TestBean.class));

        DataSink<TestBean> dataSink = d -> data.addAll(d.collectAsList());

        DataBiTransformation<Integer, TestBean, TestBean> tx = Transformation
                .<Integer, TestBean>sqlMerge("s1", "s2", "select * from (select b.value+1 as value, b.reference as reference from s1 as a join s2 as b on a.value = b.value) where value > 2")
                .andThenEncode(Encoders.bean(TestBean.class));

        // when
        DataPipe.mergeAll(DataPipe.read(dataSource1), DataPipe.read(dataSource2), tx).write(dataSink);

        // then
        Assertions.assertEquals(Collections.singletonList(TestBean.of(7, "six")), data);

    }

}