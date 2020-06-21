package dataengine.pipeline.core.source;

import dataengine.spark.transformation.DataTransformation;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

@Value
@AllArgsConstructor(staticName = "of")
@Builder
public class DataSource1<S1, D> implements DataSource<D> {

    @Nonnull
    dataengine.pipeline.core.source.DataSource<S1> parentDataSource1;
    @Nonnull
    DataTransformation<S1, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(parentDataSource1.get());
    }

}
