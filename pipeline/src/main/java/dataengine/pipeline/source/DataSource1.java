package dataengine.pipeline.source;

import dataengine.pipeline.DataSource;
import dataengine.pipeline.DataTransformation;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

@Value
@Builder
public class DataSource1<S1, D> implements DataSource<D> {

    @Nonnull
    DataSource<S1> parentDataSource1;
    @Nonnull
    DataTransformation<S1, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(parentDataSource1.get());
    }

}
