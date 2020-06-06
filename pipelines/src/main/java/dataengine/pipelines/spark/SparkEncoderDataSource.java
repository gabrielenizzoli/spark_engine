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
public class SparkEncoderDataSource<T> implements DataSource<T> {

    @Nonnull
    String format;
    @Nonnull
    @Singular
    Map<String, String> options;
    @Nonnull
    Encoder<T> encoder;

    public static class SparkEncoderDataSourceBuilder<T> {

        public SparkEncoderDataSourceBuilder<String> text() {
            return ((SparkEncoderDataSourceBuilder<String>)this).format("text").encoder(Encoders.STRING());
        }

        public SparkEncoderDataSourceBuilder<T> path(String path) {
            return option(DataSourceOptions.PATH_KEY, path);
        }

    }

    @Override
    public Dataset<T> get() {
        return SparkSession.active().read().format(format).options(options).load().as(encoder);
    }

}
