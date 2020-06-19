package dataengine.pipeline.transformation;

import dataengine.pipeline.*;
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

class TransformationsTest extends SparkSessionTest {

    @Test
    void testEncode() {

        // given
        List<Integer> data = new LinkedList<>();
        DataSource<Integer> dataSource = () -> sparkSession.createDataset(Arrays.asList(1, 2, 3, 4, 5, 6), Encoders.INT());
        DataSink<BigDecimal> dataSink = d -> data.addAll(d.collectAsList().stream().map(BigDecimal::intValue).collect(Collectors.toList()));
        DataTransformation<Integer, Row> tx1 = SqlTransformations.sql("source", "select value*2 as value from source where value = 5");

        // when
        dataSource
                .transformation(tx1)
                .transformation(Transformations.encode(Encoders.DECIMAL()))
                .write(dataSink);

        // then
        Assertions.assertEquals(Collections.singletonList(10), data);

    }


}