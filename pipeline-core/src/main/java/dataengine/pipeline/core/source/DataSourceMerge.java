package dataengine.pipeline.core.source;

import dataengine.spark.transformation.*;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * Set of utility functions to merge together up to 10 DataSources of different types.
 * If merging is needed between DataSource of same type with a reducer,
 * the {@link #reduce(DataSource, List, DataTransformation2)} function can be used
 * to reduce an undefined list of DataSources.
 */
public class DataSourceMerge {

    public static <T> DataSource<T> reduce(DataSource<T> dataSource,
                                           List<DataSource<T>> otherDataSources,
                                           DataTransformation2<T, T, T> reducer) {
        return DataSourceReducer.<T>builder()
                .dataSource(dataSource)
                .parentDataSources(otherDataSources)
                .reducer(reducer)
                .build();
    }

    public static <S, D> DataSource<D> mergeAll(
            List<DataSource<S>> sources,
            DataTransformationN<S, D> merger) {
        return DataSourceN.<S, D>builder()
                .parentDataSources(sources)
                .transformation(merger)
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
    public static <S1, S2, D> DataSource<D> mergeAll(
            DataSource<S1> source1,
            DataSource<S2> source2,
            DataTransformation2<S1, S2, D> merger) {
        return DataSource2.<S1, S2, D>builder()
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
    public static <S1, S2, S3, D> DataSource<D> mergeAll(
            DataSource<S1> source1,
            DataSource<S2> source2,
            DataSource<S3> source3,
            DataTransformation3<S1, S2, S3, D> merger) {
        return DataSource3.<S1, S2, S3, D>builder()
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
    public static <S1, S2, S3, S4, D> DataSource<D> mergeAll(
            DataSource<S1> source1,
            DataSource<S2> source2,
            DataSource<S3> source3,
            DataSource<S4> source4,
            DataTransformation4<S1, S2, S3, S4, D> merger) {
        return DataSource4.<S1, S2, S3, S4, D>builder()
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
    public static <S1, S2, S3, S4, S5, D> DataSource<D> mergeAll(
            DataSource<S1> source1,
            DataSource<S2> source2,
            DataSource<S3> source3,
            DataSource<S4> source4,
            DataSource<S5> source5,
            DataTransformation5<S1, S2, S3, S4, S5, D> merger) {
        return DataSource5.<S1, S2, S3, S4, S5, D>builder()
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
    public static <S1, S2, S3, S4, S5, S6, D> DataSource<D> mergeAll(
            DataSource<S1> source1,
            DataSource<S2> source2,
            DataSource<S3> source3,
            DataSource<S4> source4,
            DataSource<S5> source5,
            DataSource<S6> source6,
            DataTransformation6<S1, S2, S3, S4, S5, S6, D> merger) {
        return DataSource6.<S1, S2, S3, S4, S5, S6, D>builder()
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
    public static <S1, S2, S3, S4, S5, S6, S7, D> DataSource<D> mergeAll(
            DataSource<S1> source1,
            DataSource<S2> source2,
            DataSource<S3> source3,
            DataSource<S4> source4,
            DataSource<S5> source5,
            DataSource<S6> source6,
            DataSource<S7> source7,
            DataTransformation7<S1, S2, S3, S4, S5, S6, S7, D> merger) {
        return DataSource7.<S1, S2, S3, S4, S5, S6, S7, D>builder()
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
    public static <S1, S2, S3, S4, S5, S6, S7, S8, D> DataSource<D> mergeAll(
            DataSource<S1> source1,
            DataSource<S2> source2,
            DataSource<S3> source3,
            DataSource<S4> source4,
            DataSource<S5> source5,
            DataSource<S6> source6,
            DataSource<S7> source7,
            DataSource<S8> source8,
            DataTransformation8<S1, S2, S3, S4, S5, S6, S7, S8, D> merger) {
        return DataSource8.<S1, S2, S3, S4, S5, S6, S7, S8, D>builder()
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
    public static <S1, S2, S3, S4, S5, S6, S7, S8, S9, D> DataSource<D> mergeAll(
            DataSource<S1> source1,
            DataSource<S2> source2,
            DataSource<S3> source3,
            DataSource<S4> source4,
            DataSource<S5> source5,
            DataSource<S6> source6,
            DataSource<S7> source7,
            DataSource<S8> source8,
            DataSource<S9> source9,
            DataTransformation9<S1, S2, S3, S4, S5, S6, S7, S8, S9, D> merger) {
        return DataSource9.<S1, S2, S3, S4, S5, S6, S7, S8, S9, D>builder()
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
    public static <S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, D> DataSource<D> mergeAll(
            DataSource<S1> source1,
            DataSource<S2> source2,
            DataSource<S3> source3,
            DataSource<S4> source4,
            DataSource<S5> source5,
            DataSource<S6> source6,
            DataSource<S7> source7,
            DataSource<S8> source8,
            DataSource<S9> source9,
            DataSource<S10> source10,
            DataTransformation10<S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, D> merger) {
        return DataSource10.<S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, D>builder()
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
