package dataengine.pipeline.core.source;

import dataengine.spark.transformation.*;

import java.util.List;

public class DataSourceMerge {

    public static <T> dataengine.pipeline.core.source.DataSource<T> reduce(dataengine.pipeline.core.source.DataSource<T> dataSource,
                                                                           List<dataengine.pipeline.core.source.DataSource<T>> otherDataSources,
                                                                           DataTransformation2<T, T, T> reducer) {
        return dataengine.pipeline.core.source.DataSourceReducer.<T>builder()
                .dataSource(dataSource)
                .parentDataSources(otherDataSources)
                .reducer(reducer)
                .build();
    }

    /**
     * Creates a DataSource that performs an on-demand merge transformation between all the input DataSources.
     *
     * @param source1 input DataSource #1
     * @param source2 input DataSource #2
     * @param merger  transformation to apply
     * @param <S1>    type of the input DataSource #1
     * @param <S2>    type of the input DataSource #2
     * @param <D>     type of the output DataSource
     * @return outcome of the merge operation
     */
    public static <S1, S2, D> dataengine.pipeline.core.source.DataSource<D> mergeAll(
            dataengine.pipeline.core.source.DataSource<S1> source1,
            dataengine.pipeline.core.source.DataSource<S2> source2,
            DataTransformation2<S1, S2, D> merger) {
        return dataengine.pipeline.core.source.DataSource2.<S1, S2, D>builder()
                .parentDataSource1(source1)
                .parentDataSource2(source2)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DataSource that performs an on-demand merge transformation between all the input DataSources.
     *
     * @param source1 input DataSource #1
     * @param source2 input DataSource #2
     * @param source3 input DataSource #3
     * @param merger  transformation to apply
     * @param <S1>    type of the input DataSource #1
     * @param <S2>    type of the input DataSource #2
     * @param <S3>    type of the input DataSource #3
     * @param <D>     type of the output DataSource
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, D> dataengine.pipeline.core.source.DataSource<D> mergeAll(
            dataengine.pipeline.core.source.DataSource<S1> source1,
            dataengine.pipeline.core.source.DataSource<S2> source2,
            dataengine.pipeline.core.source.DataSource<S3> source3,
            DataTransformation3<S1, S2, S3, D> merger) {
        return dataengine.pipeline.core.source.DataSource3.<S1, S2, S3, D>builder()
                .parentDataSource1(source1)
                .parentDataSource2(source2)
                .parentDataSource3(source3)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DataSource that performs an on-demand merge transformation between all the input DataSources.
     *
     * @param source1 input DataSource #1
     * @param source2 input DataSource #2
     * @param source3 input DataSource #3
     * @param source4 input DataSource #4
     * @param merger  transformation to apply
     * @param <S1>    type of the input DataSource #1
     * @param <S2>    type of the input DataSource #2
     * @param <S3>    type of the input DataSource #3
     * @param <S4>    type of the input DataSource #4
     * @param <D>     type of the output DataSource
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, D> dataengine.pipeline.core.source.DataSource<D> mergeAll(
            dataengine.pipeline.core.source.DataSource<S1> source1,
            dataengine.pipeline.core.source.DataSource<S2> source2,
            dataengine.pipeline.core.source.DataSource<S3> source3,
            dataengine.pipeline.core.source.DataSource<S4> source4,
            DataTransformation4<S1, S2, S3, S4, D> merger) {
        return dataengine.pipeline.core.source.DataSource4.<S1, S2, S3, S4, D>builder()
                .parentDataSource1(source1)
                .parentDataSource2(source2)
                .parentDataSource3(source3)
                .parentDataSource4(source4)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DataSource that performs an on-demand merge transformation between all the input DataSources.
     *
     * @param source1 input DataSource #1
     * @param source2 input DataSource #2
     * @param source3 input DataSource #3
     * @param source4 input DataSource #4
     * @param source5 input DataSource #5
     * @param merger  transformation to apply
     * @param <S1>    type of the input DataSource #1
     * @param <S2>    type of the input DataSource #2
     * @param <S3>    type of the input DataSource #3
     * @param <S4>    type of the input DataSource #4
     * @param <S5>    type of the input DataSource #5
     * @param <D>     type of the output DataSource
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, D> dataengine.pipeline.core.source.DataSource<D> mergeAll(
            dataengine.pipeline.core.source.DataSource<S1> source1,
            dataengine.pipeline.core.source.DataSource<S2> source2,
            dataengine.pipeline.core.source.DataSource<S3> source3,
            dataengine.pipeline.core.source.DataSource<S4> source4,
            dataengine.pipeline.core.source.DataSource<S5> source5,
            DataTransformation5<S1, S2, S3, S4, S5, D> merger) {
        return dataengine.pipeline.core.source.DataSource5.<S1, S2, S3, S4, S5, D>builder()
                .parentDataSource1(source1)
                .parentDataSource2(source2)
                .parentDataSource3(source3)
                .parentDataSource4(source4)
                .parentDataSource5(source5)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DataSource that performs an on-demand merge transformation between all the input DataSources.
     *
     * @param source1 input DataSource #1
     * @param source2 input DataSource #2
     * @param source3 input DataSource #3
     * @param source4 input DataSource #4
     * @param source5 input DataSource #5
     * @param source6 input DataSource #6
     * @param merger  transformation to apply
     * @param <S1>    type of the input DataSource #1
     * @param <S2>    type of the input DataSource #2
     * @param <S3>    type of the input DataSource #3
     * @param <S4>    type of the input DataSource #4
     * @param <S5>    type of the input DataSource #5
     * @param <S6>    type of the input DataSource #6
     * @param <D>     type of the output DataSource
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, D> dataengine.pipeline.core.source.DataSource<D> mergeAll(
            dataengine.pipeline.core.source.DataSource<S1> source1,
            dataengine.pipeline.core.source.DataSource<S2> source2,
            dataengine.pipeline.core.source.DataSource<S3> source3,
            dataengine.pipeline.core.source.DataSource<S4> source4,
            dataengine.pipeline.core.source.DataSource<S5> source5,
            dataengine.pipeline.core.source.DataSource<S6> source6,
            DataTransformation6<S1, S2, S3, S4, S5, S6, D> merger) {
        return dataengine.pipeline.core.source.DataSource6.<S1, S2, S3, S4, S5, S6, D>builder()
                .parentDataSource1(source1)
                .parentDataSource2(source2)
                .parentDataSource3(source3)
                .parentDataSource4(source4)
                .parentDataSource5(source5)
                .parentDataSource6(source6)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DataSource that performs an on-demand merge transformation between all the input DataSources.
     *
     * @param source1 input DataSource #1
     * @param source2 input DataSource #2
     * @param source3 input DataSource #3
     * @param source4 input DataSource #4
     * @param source5 input DataSource #5
     * @param source6 input DataSource #6
     * @param source7 input DataSource #7
     * @param merger  transformation to apply
     * @param <S1>    type of the input DataSource #1
     * @param <S2>    type of the input DataSource #2
     * @param <S3>    type of the input DataSource #3
     * @param <S4>    type of the input DataSource #4
     * @param <S5>    type of the input DataSource #5
     * @param <S6>    type of the input DataSource #6
     * @param <S7>    type of the input DataSource #7
     * @param <D>     type of the output DataSource
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7, D> dataengine.pipeline.core.source.DataSource<D> mergeAll(
            dataengine.pipeline.core.source.DataSource<S1> source1,
            dataengine.pipeline.core.source.DataSource<S2> source2,
            dataengine.pipeline.core.source.DataSource<S3> source3,
            dataengine.pipeline.core.source.DataSource<S4> source4,
            dataengine.pipeline.core.source.DataSource<S5> source5,
            dataengine.pipeline.core.source.DataSource<S6> source6,
            dataengine.pipeline.core.source.DataSource<S7> source7,
            DataTransformation7<S1, S2, S3, S4, S5, S6, S7, D> merger) {
        return dataengine.pipeline.core.source.DataSource7.<S1, S2, S3, S4, S5, S6, S7, D>builder()
                .parentDataSource1(source1)
                .parentDataSource2(source2)
                .parentDataSource3(source3)
                .parentDataSource4(source4)
                .parentDataSource5(source5)
                .parentDataSource6(source6)
                .parentDataSource7(source7)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DataSource that performs an on-demand merge transformation between all the input DataSources.
     *
     * @param source1 input DataSource #1
     * @param source2 input DataSource #2
     * @param source3 input DataSource #3
     * @param source4 input DataSource #4
     * @param source5 input DataSource #5
     * @param source6 input DataSource #6
     * @param source7 input DataSource #7
     * @param source8 input DataSource #8
     * @param merger  transformation to apply
     * @param <S1>    type of the input DataSource #1
     * @param <S2>    type of the input DataSource #2
     * @param <S3>    type of the input DataSource #3
     * @param <S4>    type of the input DataSource #4
     * @param <S5>    type of the input DataSource #5
     * @param <S6>    type of the input DataSource #6
     * @param <S7>    type of the input DataSource #7
     * @param <S8>    type of the input DataSource #8
     * @param <D>     type of the output DataSource
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7, S8, D> dataengine.pipeline.core.source.DataSource<D> mergeAll(
            dataengine.pipeline.core.source.DataSource<S1> source1,
            dataengine.pipeline.core.source.DataSource<S2> source2,
            dataengine.pipeline.core.source.DataSource<S3> source3,
            dataengine.pipeline.core.source.DataSource<S4> source4,
            dataengine.pipeline.core.source.DataSource<S5> source5,
            dataengine.pipeline.core.source.DataSource<S6> source6,
            dataengine.pipeline.core.source.DataSource<S7> source7,
            dataengine.pipeline.core.source.DataSource<S8> source8,
            DataTransformation8<S1, S2, S3, S4, S5, S6, S7, S8, D> merger) {
        return dataengine.pipeline.core.source.DataSource8.<S1, S2, S3, S4, S5, S6, S7, S8, D>builder()
                .parentDataSource1(source1)
                .parentDataSource2(source2)
                .parentDataSource3(source3)
                .parentDataSource4(source4)
                .parentDataSource5(source5)
                .parentDataSource6(source6)
                .parentDataSource7(source7)
                .parentDataSource8(source8)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DataSource that performs an on-demand merge transformation between all the input DataSources.
     *
     * @param source1 input DataSource #1
     * @param source2 input DataSource #2
     * @param source3 input DataSource #3
     * @param source4 input DataSource #4
     * @param source5 input DataSource #5
     * @param source6 input DataSource #6
     * @param source7 input DataSource #7
     * @param source8 input DataSource #8
     * @param source9 input DataSource #9
     * @param merger  transformation to apply
     * @param <S1>    type of the input DataSource #1
     * @param <S2>    type of the input DataSource #2
     * @param <S3>    type of the input DataSource #3
     * @param <S4>    type of the input DataSource #4
     * @param <S5>    type of the input DataSource #5
     * @param <S6>    type of the input DataSource #6
     * @param <S7>    type of the input DataSource #7
     * @param <S8>    type of the input DataSource #8
     * @param <S9>    type of the input DataSource #9
     * @param <D>     type of the output DataSource
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7, S8, S9, D> dataengine.pipeline.core.source.DataSource<D> mergeAll(
            dataengine.pipeline.core.source.DataSource<S1> source1,
            dataengine.pipeline.core.source.DataSource<S2> source2,
            dataengine.pipeline.core.source.DataSource<S3> source3,
            dataengine.pipeline.core.source.DataSource<S4> source4,
            dataengine.pipeline.core.source.DataSource<S5> source5,
            dataengine.pipeline.core.source.DataSource<S6> source6,
            dataengine.pipeline.core.source.DataSource<S7> source7,
            dataengine.pipeline.core.source.DataSource<S8> source8,
            dataengine.pipeline.core.source.DataSource<S9> source9,
            DataTransformation9<S1, S2, S3, S4, S5, S6, S7, S8, S9, D> merger) {
        return dataengine.pipeline.core.source.DataSource9.<S1, S2, S3, S4, S5, S6, S7, S8, S9, D>builder()
                .parentDataSource1(source1)
                .parentDataSource2(source2)
                .parentDataSource3(source3)
                .parentDataSource4(source4)
                .parentDataSource5(source5)
                .parentDataSource6(source6)
                .parentDataSource7(source7)
                .parentDataSource8(source8)
                .parentDataSource9(source9)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DataSource that performs an on-demand merge transformation between all the input DataSources.
     *
     * @param source1  input DataSource #1
     * @param source2  input DataSource #2
     * @param source3  input DataSource #3
     * @param source4  input DataSource #4
     * @param source5  input DataSource #5
     * @param source6  input DataSource #6
     * @param source7  input DataSource #7
     * @param source8  input DataSource #8
     * @param source9  input DataSource #9
     * @param source10 input DataSource #10
     * @param merger   transformation to apply
     * @param <S1>     type of the input DataSource #1
     * @param <S2>     type of the input DataSource #2
     * @param <S3>     type of the input DataSource #3
     * @param <S4>     type of the input DataSource #4
     * @param <S5>     type of the input DataSource #5
     * @param <S6>     type of the input DataSource #6
     * @param <S7>     type of the input DataSource #7
     * @param <S8>     type of the input DataSource #8
     * @param <S9>     type of the input DataSource #9
     * @param <S10>    type of the input DataSource #10
     * @param <D>      type of the output DataSource
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, D> dataengine.pipeline.core.source.DataSource<D> mergeAll(
            dataengine.pipeline.core.source.DataSource<S1> source1,
            dataengine.pipeline.core.source.DataSource<S2> source2,
            dataengine.pipeline.core.source.DataSource<S3> source3,
            dataengine.pipeline.core.source.DataSource<S4> source4,
            dataengine.pipeline.core.source.DataSource<S5> source5,
            dataengine.pipeline.core.source.DataSource<S6> source6,
            dataengine.pipeline.core.source.DataSource<S7> source7,
            dataengine.pipeline.core.source.DataSource<S8> source8,
            dataengine.pipeline.core.source.DataSource<S9> source9,
            dataengine.pipeline.core.source.DataSource<S10> source10,
            DataTransformation10<S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, D> merger) {
        return dataengine.pipeline.core.source.DataSource10.<S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, D>builder()
                .parentDataSource1(source1)
                .parentDataSource2(source2)
                .parentDataSource3(source3)
                .parentDataSource4(source4)
                .parentDataSource5(source5)
                .parentDataSource6(source6)
                .parentDataSource7(source7)
                .parentDataSource8(source8)
                .parentDataSource9(source9)
                .parentDataSource10(source10)
                .transformation(merger)
                .build();
    }

}
