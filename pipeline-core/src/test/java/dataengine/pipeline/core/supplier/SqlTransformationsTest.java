package dataengine.pipeline.core.supplier;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import dataengine.pipeline.core.supplier.impl.DatasetSupplierMerge;
import dataengine.spark.test.SparkSessionBase;
import dataengine.spark.transformation.DataTransformation;
import dataengine.spark.transformation.DataTransformation2;
import dataengine.spark.transformation.Transformations;
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
        DatasetSupplier<Integer> datasetSupplier = () -> sparkSession.createDataset(Arrays.asList(1, 2, 3, 4, 5, 6), Encoders.INT());
        DatasetConsumer<BigDecimal> datasetConsumer = d -> data.addAll(d.collectAsList().stream().map(BigDecimal::intValue).collect(Collectors.toList()));

        // when
        datasetSupplier
                .sql("source", "select value*2 as value from source")
                .sql("source2", "select sum(value) as sumValue from source2")
                .encodeAs(Encoders.DECIMAL())
                .writeTo(datasetConsumer);

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
        DatasetSupplier<Integer> datasetSupplier1 = () -> sparkSession.createDataset(Arrays.asList(1, 2, 3, 4, 5, 6), Encoders.INT());
        DatasetSupplier<TestBean> datasetSupplier2 = () -> sparkSession.createDataset(Arrays.asList(TestBean.of(1, "one"), TestBean.of(6, "six")), Encoders.bean(TestBean.class));
        DatasetConsumer<TestBean> datasetConsumer = d -> data.addAll(d.collectAsList());

        DataTransformation2<Integer, TestBean, TestBean> tx = Transformations
                .<Integer, TestBean>sql("s1", "s2", "select * from (select b.value+1 as value, b.reference as reference from s1 as a join s2 as b on a.value = b.value) where value > 2")
                .andThenEncode(Encoders.bean(TestBean.class));

        // when
        DatasetSupplierMerge.mergeAll(datasetSupplier1, datasetSupplier2, tx).writeTo(datasetConsumer);

        // then
        Assertions.assertEquals(Collections.singletonList(TestBean.of(7, "six")), data);

    }

}