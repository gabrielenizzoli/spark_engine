package dataengine.pipeline.core.source;

import dataengine.spark.transformation.DataTransformation3;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

/**
 * DataSource that stores information needed to perform a custom-defined operation on 3 input DataSources
 *
 * @param <S1> type of the input DataSource #1
 * @param <S2> type of the input DataSource #2
 * @param <S3> type of the input DataSource #3
 * @param <D>  type of the output DataSource
 */
@Value
@Builder
public class DataSource3<S1, S2, S3, D> implements dataengine.pipeline.core.source.DataSource<D> {

    @Nonnull
    dataengine.pipeline.core.source.DataSource<S1> parentDataSource1;
    @Nonnull
    dataengine.pipeline.core.source.DataSource<S2> parentDataSource2;
    @Nonnull
    dataengine.pipeline.core.source.DataSource<S3> parentDataSource3;
    @Nonnull
    DataTransformation3<S1, S2, S3, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(
                parentDataSource1.get(),
                parentDataSource2.get(),
                parentDataSource3.get()
        );
    }
}
