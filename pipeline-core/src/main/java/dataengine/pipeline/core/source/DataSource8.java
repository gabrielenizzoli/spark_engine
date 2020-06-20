package dataengine.pipeline.core.source;

import dataengine.spark.transformation.DataTransformation8;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

/**
 * DataSource that stores information needed to perform a custom-defined operation on 8 input DataSources
 *
 * @param <S1> type of the input DataSource #1
 * @param <S2> type of the input DataSource #2
 * @param <S3> type of the input DataSource #3
 * @param <S4> type of the input DataSource #4
 * @param <S5> type of the input DataSource #5
 * @param <S6> type of the input DataSource #6
 * @param <S7> type of the input DataSource #7
 * @param <S8> type of the input DataSource #8
 * @param <D>  type of the output DataSource
 */
@Value
@Builder
public class DataSource8<S1, S2, S3, S4, S5, S6, S7, S8, D> implements DataSource<D> {

    @Nonnull
    DataSource<S1> parentDataSource1;
    @Nonnull
    DataSource<S2> parentDataSource2;
    @Nonnull
    DataSource<S3> parentDataSource3;
    @Nonnull
    DataSource<S4> parentDataSource4;
    @Nonnull
    DataSource<S5> parentDataSource5;
    @Nonnull
    DataSource<S6> parentDataSource6;
    @Nonnull
    DataSource<S7> parentDataSource7;
    @Nonnull
    DataSource<S8> parentDataSource8;
    @Nonnull
    DataTransformation8<S1, S2, S3, S4, S5, S6, S7, S8, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(
                parentDataSource1.get(),
                parentDataSource2.get(),
                parentDataSource3.get(),
                parentDataSource4.get(),
                parentDataSource5.get(),
                parentDataSource6.get(),
                parentDataSource7.get(),
                parentDataSource8.get()
        );
    }
}
