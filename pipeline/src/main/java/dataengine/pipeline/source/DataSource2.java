package dataengine.pipeline.source;

import dataengine.pipeline.Data2Transformation;
import dataengine.pipeline.DataSource;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

@Value
@Builder
public class DataSource2<S1, S2, D> implements DataSource<D> {

    @Nonnull
    DataSource<S1> parentDataSource1;
    @Nonnull
    DataSource<S2> parentDataSource2;
    @Nonnull
    Data2Transformation<S1, S2, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(
                parentDataSource1.get(),
                parentDataSource2.get()
        );
    }

}
