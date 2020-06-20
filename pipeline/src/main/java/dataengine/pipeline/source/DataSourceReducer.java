package dataengine.pipeline.source;

import dataengine.pipeline.Data2Transformation;
import dataengine.pipeline.DataSource;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Supplier;

@Value
@Builder
public class DataSourceReducer<T> implements DataSource<T> {

    @Nonnull
    DataSource<T> dataSource;
    @Nonnull
    @Singular
    List<DataSource<T>> parentDataSources;
    @Nonnull
    Data2Transformation<T, T, T> reducer;

    @Override
    public Dataset<T> get() {
        return parentDataSources.stream().map(Supplier::get).reduce(dataSource.get(), reducer::apply);
    }

}
