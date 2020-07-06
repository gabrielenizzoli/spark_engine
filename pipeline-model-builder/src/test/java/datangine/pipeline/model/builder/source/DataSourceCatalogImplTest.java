package datangine.pipeline.model.builder.source;

import dataengine.pipeline.core.sink.impl.DataSinkCollectRows;
import dataengine.pipeline.core.source.factory.DataSourceCatalog;
import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.model.builder.source.DataSourceCatalogImpl;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

class DataSourceCatalogImplTest extends SparkSessionBase {

    @Test
    void testBuilder() throws DataSourceCatalogException {

        // given
        ComponentCatalog steps = Utils.getCatalog();
        DataSourceCatalog dataSourceCatalog = DataSourceCatalogImpl.withComponentCatalog(steps);

        // when
        DataSinkCollectRows<Row> dataSink = new DataSinkCollectRows<>();
        dataSourceCatalog.lookup("tx").encodeAsRow().writeTo(dataSink);

        // then
        Assertions.assertEquals(
                Arrays.asList("a:xxx", "b:yyy"),
                dataSink.getRows().stream()
                        .map(r -> r.get(r.fieldIndex("str")) + ":" + r.getString(r.fieldIndex("str2")))
                        .sorted()
                        .collect(Collectors.toList())
        );
    }

}