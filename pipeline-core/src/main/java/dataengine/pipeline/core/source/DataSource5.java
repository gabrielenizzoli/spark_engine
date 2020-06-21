package dataengine.pipeline.core.source;

import dataengine.spark.transformation.DataTransformation5;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

/**
 * DataSource that stores information needed to perform a custom-defined operation on 5 input DataSources
 *
 * @param <S1> type of the input DataSource #1
 * @param <S2> type of the input DataSource #2
 * @param <S3> type of the input DataSource #3
 * @param <S4> type of the input DataSource #4
 * @param <S5> type of the input DataSource #5
 * @param <D>  type of the output DataSource
 */
@Value
@Builder
public class DataSource5<S1, S2, S3, S4, S5, D> implements DataSource<D> {

    @Nonnull
    dataengine.pipeline.core.source.DataSource<S1> parentDataSource1;
    @Nonnull
    dataengine.pipeline.core.source.DataSource<S2> parentDataSource2;
    @Nonnull
    dataengine.pipeline.core.source.DataSource<S3> parentDataSource3;
    @Nonnull
    dataengine.pipeline.core.source.DataSource<S4> parentDataSource4;
    @Nonnull
    dataengine.pipeline.core.source.DataSource<S5> parentDataSource5;
    @Nonnull
    DataTransformation5<S1, S2, S3, S4, S5, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(
                parentDataSource1.get(),
                parentDataSource2.get(),
                parentDataSource3.get(),
                parentDataSource4.get(),
                parentDataSource5.get()
        );
    }
}
