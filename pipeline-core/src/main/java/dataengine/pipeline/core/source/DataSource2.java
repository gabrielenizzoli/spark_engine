package dataengine.pipeline.core.source;

import dataengine.spark.transformation.DataTransformation2;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

/**
 * DataSource that stores information needed to perform a custom-defined operation on 2 input DataSources
 *
 * @param <S1> type of the input DataSource #1
 * @param <S2> type of the input DataSource #2
 * @param <D>  type of the output DataSource
 */
@Value
@Builder
public class DataSource2<S1, S2, D> implements DataSource<D> {

    @Nonnull
    DataSource<S1> parentDataSource1;
    @Nonnull
    DataSource<S2> parentDataSource2;
    @Nonnull
    DataTransformation2<S1, S2, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(
                parentDataSource1.get(),
                parentDataSource2.get()
        );
    }
}
