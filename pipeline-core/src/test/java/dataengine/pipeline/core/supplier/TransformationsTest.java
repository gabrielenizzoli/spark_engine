package dataengine.pipeline.core.supplier;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import dataengine.pipeline.core.supplier.impl.DatasetSupplier2;
import dataengine.pipeline.core.supplier.impl.InlineObjectSupplier;
import dataengine.spark.test.SparkSessionBase;
import dataengine.spark.transformation.DataTransformation;
import dataengine.spark.transformation.DataTransformation2;
import dataengine.spark.transformation.Transformations;
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

class TransformationsTest extends SparkSessionBase {

    @Test
    void testEncode() {

        // given
        List<Integer> data = new LinkedList<>();
        DatasetSupplier<Integer> datasetSupplier = () -> sparkSession.createDataset(Arrays.asList(1, 2, 3, 4, 5, 6), Encoders.INT());
        DatasetConsumer<BigDecimal> datasetConsumer = d -> data.addAll(d.collectAsList().stream().map(BigDecimal::intValue).collect(Collectors.toList()));
        DataTransformation<Integer, Row> tx1 = Transformations.sql("source", "select value*2 as value from source where value = 5");

        // when
        datasetSupplier
                .transform(tx1)
                .transform(Transformations.encodeAs(Encoders.DECIMAL()))
                .writeTo(datasetConsumer);

        // then
        Assertions.assertEquals(Collections.singletonList(10), data);

    }

    @Test
    void testJoin() {

        // given
        var supplier1 = InlineObjectSupplier.<Integer>builder()
                .objects(List.of(1, 2, 3, 5))
                .encoder(Encoders.INT()).build();

        var supplier2 = InlineObjectSupplier.<Integer>builder()
                .objects(List.of(3, 4, 5))
                .encoder(Encoders.INT()).build();

        DataTransformation2<Integer, Integer, Row> join = Transformations.sql("source1", "source2", "select source1.value from source1 join source2 on source1.value = source2.value");

        // when
        var joinSupplier = DatasetSupplier2.<Integer, Integer, Row>builder()
                .parentDatasetSupplier1(supplier1)
                .parentDatasetSupplier2(supplier2)
                .transformation(join)
                .build();

        // then
        Assertions.assertEquals(List.of(3, 5), joinSupplier.get().select("value").as(Encoders.INT()).collectAsList());
    }


}