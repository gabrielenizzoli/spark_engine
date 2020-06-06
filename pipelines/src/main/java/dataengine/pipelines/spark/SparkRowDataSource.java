package dataengine.pipelines.spark;

import dataengine.pipelines.DataSource;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

import javax.annotation.Nonnull;
import java.util.Map;

@Value
@Builder
public class SparkRowDataSource implements DataSource<Row> {

    @Nonnull
    String format;
    @Nonnull
    @Singular
    Map<String, String> options;

    public static class SparkRowDataSourceBuilder {

        public SparkRowDataSourceBuilder json() {
            return format("json");
        }

        public SparkRowDataSourceBuilder path(String path) {
            return option(DataSourceOptions.PATH_KEY, path);
        }

    }

    @Override
    public Dataset<Row> get() {
        return SparkSession.active().read().format(format).options(options).load();
    }

}
