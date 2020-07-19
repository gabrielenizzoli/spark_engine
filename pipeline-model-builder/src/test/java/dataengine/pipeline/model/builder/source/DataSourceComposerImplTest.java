package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.sink.impl.DataSinkCollectRows;
import dataengine.pipeline.core.source.composer.DataSourceComposer;
import dataengine.pipeline.core.source.composer.DataSourceComposerException;
import dataengine.pipeline.core.source.composer.DataSourceComposerImpl;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

class DataSourceComposerImplTest extends SparkSessionBase {

    @Test
    void testBuilder() throws DataSourceComposerException {

        // given
        DataSourceComposerImpl dataSourceComposer = DataSourceComposerImpl
                .ofCatalog(TestUtils.getComponentCatalog(null));

        // when
        DataSinkCollectRows<Row> dataSink = new DataSinkCollectRows<>();
        dataSourceComposer.lookup("tx").encodeAsRow().writeTo(dataSink);

        // then
        Assertions.assertEquals(
                Arrays.asList("a:xxx", "b:yyy"),
                dataSink.getRows().stream()
                        .map(r -> r.get(r.fieldIndex("str")) + ":" + r.getString(r.fieldIndex("str2")))
                        .sorted()
                        .collect(Collectors.toList())
        );
    }

    @Test
    void testBuilderWithBadComponents() {

        Assertions.assertThrows(DataSourceComposerException.class, () -> {
            DataSourceComposer dataSourceComposer = DataSourceComposerImpl.ofCatalog(TestUtils.getComponentCatalog("testBadComponentsCatalog"));
            dataSourceComposer.lookup("tx");
        });

    }

}