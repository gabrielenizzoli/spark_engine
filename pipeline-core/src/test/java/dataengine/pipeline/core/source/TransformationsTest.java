package dataengine.pipeline.core.source;

import dataengine.pipeline.core.SparkSessionBase;
import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.source.DataSource;
import dataengine.spark.transformation.DataTransformation;
import dataengine.spark.transformation.SqlTransformations;
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
        DataSource<Integer> dataSource = () -> sparkSession.createDataset(Arrays.asList(1, 2, 3, 4, 5, 6), Encoders.INT());
        DataSink<BigDecimal> dataSink = d -> data.addAll(d.collectAsList().stream().map(BigDecimal::intValue).collect(Collectors.toList()));
        DataTransformation<Integer, Row> tx1 = SqlTransformations.sql("source", "select value*2 as value from source where value = 5");

        // when
        dataSource
                .transform(tx1)
                .transform(Transformations.encode(Encoders.DECIMAL()))
                .write(dataSink);

        // then
        Assertions.assertEquals(Collections.singletonList(10), data);

    }


}