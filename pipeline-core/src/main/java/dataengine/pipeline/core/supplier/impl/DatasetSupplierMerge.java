package dataengine.pipeline.core.supplier.impl;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.spark.transformation.*;

import java.util.List;

/**
 * Set of utility functions to merge together up to 10 DatasetSuppliers of different types.
 * If merging is needed between DatasetSupplier of same type with a reducer,
 * the {@link #reduce(DatasetSupplier, List, DataTransformation2)} function can be used
 * to reduce an undefined list of DatasetSuppliers.
 */
public class DatasetSupplierMerge {

    public static <T> DatasetSupplier<T> reduce(DatasetSupplier<T> datasetSupplier,
                                                List<DatasetSupplier<T>> otherDatasetSuppliers,
                                                DataTransformation2<T, T, T> reducer) {
        return DatasetSupplierReducer.<T>builder()
                .datasetSupplier(datasetSupplier)
                .parentDatasetSuppliers(otherDatasetSuppliers)
                .reducer(reducer)
                .build();
    }

    public static <S, D> DatasetSupplier<D> mergeAll(
            List<DatasetSupplier<S>> sources,
            DataTransformationN<S, D> merger) {
        return DatasetSupplierN.<S, D>builder()
                .parentDatasetSuppliers(sources)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DatasetSupplier that performs an on-demand merge transformation between all the input DatasetSuppliers.
     *
     * @param source1 input DatasetSupplier #1
     * @param source2 input DatasetSupplier #2
     * @param merger  transformation to apply
     * @param <S1>    type of the input DatasetSupplier #1
     * @param <S2>    type of the input DatasetSupplier #2
     * @param <D>     type of the output DatasetSupplier
     * @return outcome of the merge operation
     */
    public static <S1, S2, D> DatasetSupplier<D> mergeAll(
            DatasetSupplier<S1> source1,
            DatasetSupplier<S2> source2,
            DataTransformation2<S1, S2, D> merger) {
        return DatasetSupplier2.<S1, S2, D>builder()
                .parentDatasetSupplier1(source1)
                .parentDatasetSupplier2(source2)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DatasetSupplier that performs an on-demand merge transformation between all the input DatasetSuppliers.
     *
     * @param source1 input DatasetSupplier #1
     * @param source2 input DatasetSupplier #2
     * @param source3 input DatasetSupplier #3
     * @param merger  transformation to apply
     * @param <S1>    type of the input DatasetSupplier #1
     * @param <S2>    type of the input DatasetSupplier #2
     * @param <S3>    type of the input DatasetSupplier #3
     * @param <D>     type of the output DatasetSupplier
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, D> DatasetSupplier<D> mergeAll(
            DatasetSupplier<S1> source1,
            DatasetSupplier<S2> source2,
            DatasetSupplier<S3> source3,
            DataTransformation3<S1, S2, S3, D> merger) {
        return DatasetSupplier3.<S1, S2, S3, D>builder()
                .parentDatasetSupplier1(source1)
                .parentDatasetSupplier2(source2)
                .parentDatasetSupplier3(source3)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DatasetSupplier that performs an on-demand merge transformation between all the input DatasetSuppliers.
     *
     * @param source1 input DatasetSupplier #1
     * @param source2 input DatasetSupplier #2
     * @param source3 input DatasetSupplier #3
     * @param source4 input DatasetSupplier #4
     * @param merger  transformation to apply
     * @param <S1>    type of the input DatasetSupplier #1
     * @param <S2>    type of the input DatasetSupplier #2
     * @param <S3>    type of the input DatasetSupplier #3
     * @param <S4>    type of the input DatasetSupplier #4
     * @param <D>     type of the output DatasetSupplier
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, D> DatasetSupplier<D> mergeAll(
            DatasetSupplier<S1> source1,
            DatasetSupplier<S2> source2,
            DatasetSupplier<S3> source3,
            DatasetSupplier<S4> source4,
            DataTransformation4<S1, S2, S3, S4, D> merger) {
        return DatasetSupplier4.<S1, S2, S3, S4, D>builder()
                .parentDatasetSupplier1(source1)
                .parentDatasetSupplier2(source2)
                .parentDatasetSupplier3(source3)
                .parentDatasetSupplier4(source4)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DatasetSupplier that performs an on-demand merge transformation between all the input DatasetSuppliers.
     *
     * @param source1 input DatasetSupplier #1
     * @param source2 input DatasetSupplier #2
     * @param source3 input DatasetSupplier #3
     * @param source4 input DatasetSupplier #4
     * @param source5 input DatasetSupplier #5
     * @param merger  transformation to apply
     * @param <S1>    type of the input DatasetSupplier #1
     * @param <S2>    type of the input DatasetSupplier #2
     * @param <S3>    type of the input DatasetSupplier #3
     * @param <S4>    type of the input DatasetSupplier #4
     * @param <S5>    type of the input DatasetSupplier #5
     * @param <D>     type of the output DatasetSupplier
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, D> DatasetSupplier<D> mergeAll(
            DatasetSupplier<S1> source1,
            DatasetSupplier<S2> source2,
            DatasetSupplier<S3> source3,
            DatasetSupplier<S4> source4,
            DatasetSupplier<S5> source5,
            DataTransformation5<S1, S2, S3, S4, S5, D> merger) {
        return DatasetSupplier5.<S1, S2, S3, S4, S5, D>builder()
                .parentDatasetSupplier1(source1)
                .parentDatasetSupplier2(source2)
                .parentDatasetSupplier3(source3)
                .parentDatasetSupplier4(source4)
                .parentDatasetSupplier5(source5)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DatasetSupplier that performs an on-demand merge transformation between all the input DatasetSuppliers.
     *
     * @param source1 input DatasetSupplier #1
     * @param source2 input DatasetSupplier #2
     * @param source3 input DatasetSupplier #3
     * @param source4 input DatasetSupplier #4
     * @param source5 input DatasetSupplier #5
     * @param source6 input DatasetSupplier #6
     * @param merger  transformation to apply
     * @param <S1>    type of the input DatasetSupplier #1
     * @param <S2>    type of the input DatasetSupplier #2
     * @param <S3>    type of the input DatasetSupplier #3
     * @param <S4>    type of the input DatasetSupplier #4
     * @param <S5>    type of the input DatasetSupplier #5
     * @param <S6>    type of the input DatasetSupplier #6
     * @param <D>     type of the output DatasetSupplier
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, D> DatasetSupplier<D> mergeAll(
            DatasetSupplier<S1> source1,
            DatasetSupplier<S2> source2,
            DatasetSupplier<S3> source3,
            DatasetSupplier<S4> source4,
            DatasetSupplier<S5> source5,
            DatasetSupplier<S6> source6,
            DataTransformation6<S1, S2, S3, S4, S5, S6, D> merger) {
        return DatasetSupplier6.<S1, S2, S3, S4, S5, S6, D>builder()
                .parentDatasetSupplier1(source1)
                .parentDatasetSupplier2(source2)
                .parentDatasetSupplier3(source3)
                .parentDatasetSupplier4(source4)
                .parentDatasetSupplier5(source5)
                .parentDatasetSupplier6(source6)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DatasetSupplier that performs an on-demand merge transformation between all the input DatasetSuppliers.
     *
     * @param source1 input DatasetSupplier #1
     * @param source2 input DatasetSupplier #2
     * @param source3 input DatasetSupplier #3
     * @param source4 input DatasetSupplier #4
     * @param source5 input DatasetSupplier #5
     * @param source6 input DatasetSupplier #6
     * @param source7 input DatasetSupplier #7
     * @param merger  transformation to apply
     * @param <S1>    type of the input DatasetSupplier #1
     * @param <S2>    type of the input DatasetSupplier #2
     * @param <S3>    type of the input DatasetSupplier #3
     * @param <S4>    type of the input DatasetSupplier #4
     * @param <S5>    type of the input DatasetSupplier #5
     * @param <S6>    type of the input DatasetSupplier #6
     * @param <S7>    type of the input DatasetSupplier #7
     * @param <D>     type of the output DatasetSupplier
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7, D> DatasetSupplier<D> mergeAll(
            DatasetSupplier<S1> source1,
            DatasetSupplier<S2> source2,
            DatasetSupplier<S3> source3,
            DatasetSupplier<S4> source4,
            DatasetSupplier<S5> source5,
            DatasetSupplier<S6> source6,
            DatasetSupplier<S7> source7,
            DataTransformation7<S1, S2, S3, S4, S5, S6, S7, D> merger) {
        return DatasetSupplier7.<S1, S2, S3, S4, S5, S6, S7, D>builder()
                .parentDatasetSupplier1(source1)
                .parentDatasetSupplier2(source2)
                .parentDatasetSupplier3(source3)
                .parentDatasetSupplier4(source4)
                .parentDatasetSupplier5(source5)
                .parentDatasetSupplier6(source6)
                .parentDatasetSupplier7(source7)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DatasetSupplier that performs an on-demand merge transformation between all the input DatasetSuppliers.
     *
     * @param source1 input DatasetSupplier #1
     * @param source2 input DatasetSupplier #2
     * @param source3 input DatasetSupplier #3
     * @param source4 input DatasetSupplier #4
     * @param source5 input DatasetSupplier #5
     * @param source6 input DatasetSupplier #6
     * @param source7 input DatasetSupplier #7
     * @param source8 input DatasetSupplier #8
     * @param merger  transformation to apply
     * @param <S1>    type of the input DatasetSupplier #1
     * @param <S2>    type of the input DatasetSupplier #2
     * @param <S3>    type of the input DatasetSupplier #3
     * @param <S4>    type of the input DatasetSupplier #4
     * @param <S5>    type of the input DatasetSupplier #5
     * @param <S6>    type of the input DatasetSupplier #6
     * @param <S7>    type of the input DatasetSupplier #7
     * @param <S8>    type of the input DatasetSupplier #8
     * @param <D>     type of the output DatasetSupplier
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7, S8, D> DatasetSupplier<D> mergeAll(
            DatasetSupplier<S1> source1,
            DatasetSupplier<S2> source2,
            DatasetSupplier<S3> source3,
            DatasetSupplier<S4> source4,
            DatasetSupplier<S5> source5,
            DatasetSupplier<S6> source6,
            DatasetSupplier<S7> source7,
            DatasetSupplier<S8> source8,
            DataTransformation8<S1, S2, S3, S4, S5, S6, S7, S8, D> merger) {
        return DatasetSupplier8.<S1, S2, S3, S4, S5, S6, S7, S8, D>builder()
                .parentDatasetSupplier1(source1)
                .parentDatasetSupplier2(source2)
                .parentDatasetSupplier3(source3)
                .parentDatasetSupplier4(source4)
                .parentDatasetSupplier5(source5)
                .parentDatasetSupplier6(source6)
                .parentDatasetSupplier7(source7)
                .parentDatasetSupplier8(source8)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DatasetSupplier that performs an on-demand merge transformation between all the input DatasetSuppliers.
     *
     * @param source1 input DatasetSupplier #1
     * @param source2 input DatasetSupplier #2
     * @param source3 input DatasetSupplier #3
     * @param source4 input DatasetSupplier #4
     * @param source5 input DatasetSupplier #5
     * @param source6 input DatasetSupplier #6
     * @param source7 input DatasetSupplier #7
     * @param source8 input DatasetSupplier #8
     * @param source9 input DatasetSupplier #9
     * @param merger  transformation to apply
     * @param <S1>    type of the input DatasetSupplier #1
     * @param <S2>    type of the input DatasetSupplier #2
     * @param <S3>    type of the input DatasetSupplier #3
     * @param <S4>    type of the input DatasetSupplier #4
     * @param <S5>    type of the input DatasetSupplier #5
     * @param <S6>    type of the input DatasetSupplier #6
     * @param <S7>    type of the input DatasetSupplier #7
     * @param <S8>    type of the input DatasetSupplier #8
     * @param <S9>    type of the input DatasetSupplier #9
     * @param <D>     type of the output DatasetSupplier
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7, S8, S9, D> DatasetSupplier<D> mergeAll(
            DatasetSupplier<S1> source1,
            DatasetSupplier<S2> source2,
            DatasetSupplier<S3> source3,
            DatasetSupplier<S4> source4,
            DatasetSupplier<S5> source5,
            DatasetSupplier<S6> source6,
            DatasetSupplier<S7> source7,
            DatasetSupplier<S8> source8,
            DatasetSupplier<S9> source9,
            DataTransformation9<S1, S2, S3, S4, S5, S6, S7, S8, S9, D> merger) {
        return DatasetSupplier9.<S1, S2, S3, S4, S5, S6, S7, S8, S9, D>builder()
                .parentDatasetSupplier1(source1)
                .parentDatasetSupplier2(source2)
                .parentDatasetSupplier3(source3)
                .parentDatasetSupplier4(source4)
                .parentDatasetSupplier5(source5)
                .parentDatasetSupplier6(source6)
                .parentDatasetSupplier7(source7)
                .parentDatasetSupplier8(source8)
                .parentDatasetSupplier9(source9)
                .transformation(merger)
                .build();
    }

    /**
     * Creates a DatasetSupplier that performs an on-demand merge transformation between all the input DatasetSuppliers.
     *
     * @param source1  input DatasetSupplier #1
     * @param source2  input DatasetSupplier #2
     * @param source3  input DatasetSupplier #3
     * @param source4  input DatasetSupplier #4
     * @param source5  input DatasetSupplier #5
     * @param source6  input DatasetSupplier #6
     * @param source7  input DatasetSupplier #7
     * @param source8  input DatasetSupplier #8
     * @param source9  input DatasetSupplier #9
     * @param source10 input DatasetSupplier #10
     * @param merger   transformation to apply
     * @param <S1>     type of the input DatasetSupplier #1
     * @param <S2>     type of the input DatasetSupplier #2
     * @param <S3>     type of the input DatasetSupplier #3
     * @param <S4>     type of the input DatasetSupplier #4
     * @param <S5>     type of the input DatasetSupplier #5
     * @param <S6>     type of the input DatasetSupplier #6
     * @param <S7>     type of the input DatasetSupplier #7
     * @param <S8>     type of the input DatasetSupplier #8
     * @param <S9>     type of the input DatasetSupplier #9
     * @param <S10>    type of the input DatasetSupplier #10
     * @param <D>      type of the output DatasetSupplier
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, D> DatasetSupplier<D> mergeAll(
            DatasetSupplier<S1> source1,
            DatasetSupplier<S2> source2,
            DatasetSupplier<S3> source3,
            DatasetSupplier<S4> source4,
            DatasetSupplier<S5> source5,
            DatasetSupplier<S6> source6,
            DatasetSupplier<S7> source7,
            DatasetSupplier<S8> source8,
            DatasetSupplier<S9> source9,
            DatasetSupplier<S10> source10,
            DataTransformation10<S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, D> merger) {
        return DatasetSupplier10.<S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, D>builder()
                .parentDatasetSupplier1(source1)
                .parentDatasetSupplier2(source2)
                .parentDatasetSupplier3(source3)
                .parentDatasetSupplier4(source4)
                .parentDatasetSupplier5(source5)
                .parentDatasetSupplier6(source6)
                .parentDatasetSupplier7(source7)
                .parentDatasetSupplier8(source8)
                .parentDatasetSupplier9(source9)
                .parentDatasetSupplier10(source10)
                .transformation(merger)
                .build();
    }

}
