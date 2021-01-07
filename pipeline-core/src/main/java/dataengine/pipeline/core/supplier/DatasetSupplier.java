package dataengine.pipeline.core.supplier;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import dataengine.pipeline.core.consumer.impl.ShowConsumer;
import dataengine.pipeline.core.supplier.impl.DatasetSupplier1;
import dataengine.pipeline.core.supplier.impl.DatasetSupplierMerge;
import dataengine.spark.transformation.DataTransformation;
import dataengine.spark.transformation.DataTransformation2;
import dataengine.spark.transformation.Transformations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public interface DatasetSupplier<T> extends Supplier<Dataset<T>> {

    default DatasetConsumer<T> writeTo(DatasetConsumer<T> consumer) {
        consumer.accept(get());
        return consumer;
    }

    default <D> DatasetSupplier<D> transform(DataTransformation<T, D> transformation) {
        return DatasetSupplier1.of(this, transformation);
    }

    default DatasetSupplier<Row> sql(String sourceName, String sql) {
        return transform(Transformations.sql(sourceName, sql));
    }


    /**
     * Provides a Dataset that has his data cached.
     *
     * @param storageLevel caching level desired
     * @return DatasetSupplier with a dataset that caches data
     */
    default DatasetSupplier<T> cache(StorageLevel storageLevel) {
        return transform(Transformations.cache(storageLevel));
    }

    default <D> DatasetSupplier<D> encodeAs(Encoder<D> encoder) {
        return transform(Transformations.encodeAs(encoder));
    }

    /**
     * Encode to Row (in spark terms: a DataFrame).
     *
     * @return DataFrame
     */
    default DatasetSupplier<Row> encodeAsRow() {
        return transform(Transformations.encodeAsRow());
    }

    /**
     * 2-way merge operation between data provided by this DatasetSupplier and another one.
     *
     * @param otherDatasetSupplier Second DatasetSupplier
     * @param merger               Merge operation
     * @param <T2>                 Type of second DatasetSupplier.
     * @param <D>                  Return type of merge operation.
     * @return Resulting DatasetSupplier
     */
    default <T2, D> DatasetSupplier<D> mergeWith(DatasetSupplier<T2> otherDatasetSupplier, DataTransformation2<T, T2, D> merger) {
        return DatasetSupplierMerge.mergeAll(this, otherDatasetSupplier, merger);
    }

    default DatasetSupplier<T> reduce(List<DatasetSupplier<T>> otherDatasetSuppliers, DataTransformation2<T, T, T> reducer) {
        return DatasetSupplierMerge.reduce(this, otherDatasetSuppliers, reducer);
    }

    default DatasetSupplier<T> union(DatasetSupplier<T>... otherDatasetSuppliers) {
        return reduce(Arrays.asList(otherDatasetSuppliers), Dataset::union);
    }

    default DatasetSupplier<T> union(List<DatasetSupplier<T>> otherDatasetSuppliers) {
        return reduce(otherDatasetSuppliers, Dataset::union);
    }

    default void show() {
        writeTo(ShowConsumer.<T>builder().build());
    }

}
