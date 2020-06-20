package dataengine.pipeline.core.source;

import dataengine.spark.transformation.DataTransformation10;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

/**
 * DataSource that stores information needed to perform a custom-defined operation on 10 input DataSources
 *
 * @param <S1>  type of the input DataSource #1
 * @param <S2>  type of the input DataSource #2
 * @param <S3>  type of the input DataSource #3
 * @param <S4>  type of the input DataSource #4
 * @param <S5>  type of the input DataSource #5
 * @param <S6>  type of the input DataSource #6
 * @param <S7>  type of the input DataSource #7
 * @param <S8>  type of the input DataSource #8
 * @param <S9>  type of the input DataSource #9
 * @param <S10> type of the input DataSource #10
 * @param <D>   type of the output DataSource
 */
@Value
@Builder
public class DataSource10<S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, D> implements dataengine.pipeline.core.source.DataSource<D> {

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
    dataengine.pipeline.core.source.DataSource<S6> parentDataSource6;
    @Nonnull
    dataengine.pipeline.core.source.DataSource<S7> parentDataSource7;
    @Nonnull
    dataengine.pipeline.core.source.DataSource<S8> parentDataSource8;
    @Nonnull
    dataengine.pipeline.core.source.DataSource<S9> parentDataSource9;
    @Nonnull
    dataengine.pipeline.core.source.DataSource<S10> parentDataSource10;
    @Nonnull
    DataTransformation10<S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, D> transformation;

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
                parentDataSource8.get(),
                parentDataSource9.get(),
                parentDataSource10.get()
        );
    }
}
