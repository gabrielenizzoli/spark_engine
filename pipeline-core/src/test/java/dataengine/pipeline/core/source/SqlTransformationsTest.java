package dataengine.pipeline.core.source;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.spark.test.SparkSessionBase;
import dataengine.spark.transformation.DataTransformation;
import dataengine.spark.transformation.DataTransformation2;
import dataengine.spark.transformation.SqlTransformations;
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

class SqlTransformationsTest extends SparkSessionBase {

    @Test
    void testSql() {

        // given
        List<Integer> data = new LinkedList<>();
        DataSource<Integer> dataSource = () -> sparkSession.createDataset(Arrays.asList(1, 2, 3, 4, 5, 6), Encoders.INT());
        DataSink<BigDecimal> dataSink = d -> data.addAll(d.collectAsList().stream().map(BigDecimal::intValue).collect(Collectors.toList()));
        DataTransformation<Integer, Row> tx1 = SqlTransformations.sql("source", "select value*2 as value from source");
        DataTransformation<Row, Row> tx2 = SqlTransformations.sql("source2", "select sum(value) as sumValue from source2");

        // when
        dataSource
                .transform(tx1)
                .transform(tx2)
                .encodeAs(Encoders.DECIMAL())
                .writeTo(dataSink);

        // then
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

        DataTransformation2<Integer, TestBean, TestBean> tx = SqlTransformations
                .<Integer, TestBean>sqlMerge("s1", "s2", "select * from (select b.value+1 as value, b.reference as reference from s1 as a join s2 as b on a.value = b.value) where value > 2")
                .andThenEncode(Encoders.bean(TestBean.class));

        // when
        DataSourceMerge.mergeAll(dataSource1, dataSource2, tx).writeTo(dataSink);

        // then
        Assertions.assertEquals(Collections.singletonList(TestBean.of(7, "six")), data);

    }

}