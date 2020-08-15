package dataengine.pipeline.core.source;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.impl.DataSinkShow;
import dataengine.spark.transformation.DataTransformation;
import dataengine.spark.transformation.DataTransformation2;
import dataengine.spark.transformation.SqlTransformations;
import dataengine.spark.transformation.Transformations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface DataSource<T> extends Supplier<Dataset<T>> {

    default void writeTo(DataSink<T> destination) {
        destination.accept(get());
    }

    default <D> DataSource<D> transform(DataTransformation<T, D> transformation) {
        return DataSource1.of(this, transformation);
    }

    /**
     * Provides a Dataset that has his data cached.
     *
     * @param storageLevel caching level desired
     * @return DataSource with a dataset that caches data
     */
    default DataSource<T> cache(StorageLevel storageLevel) {
        return transform(Transformations.cache(storageLevel));
    }

    /**
     * DataSource that provides the same Dataset. Note that the Dataset data will not be cached.
     *
     * @return stored Dataset
     */
    default DataSource<T> store() {
        return transform(Transformations.store());
    }

    default <D> DataSource<D> encodeAs(Encoder<D> encoder) {
        return transform(Transformations.encodeAs(encoder));
    }

    /**
     * Encode to Row (in spark terms: a DataFrame).
     *
     * @return DataFrame
     */
    default DataSource<Row> encodeAsRow() {
        return transform(Transformations.encodeAsRow());
    }

    /**
     * 2-way merge operation between data provided by this DataSource and another one.
     *
     * @param otherDataSource Second DataSource
     * @param merger Merge operation
     * @param <T2> Type of second DataSource.
     * @param <D> Return type of merge operation.
     * @return Resulting DataSource
     */
    default <T2, D> DataSource<D> mergeWith(DataSource<T2> otherDataSource, DataTransformation2<T, T2, D> merger) {
        return DataSourceMerge.mergeAll(this, otherDataSource, merger);
    }

    default DataSource<T> reduce(List<DataSource<T>> otherDataSources, DataTransformation2<T, T, T> reducer) {
        return DataSourceMerge.reduce(this, otherDataSources, reducer);
    }

    default DataSource<T> union(DataSource<T>... otherDataSources) {
        return reduce(Arrays.asList(otherDataSources), Dataset::union);
    }

    default DataSource<T> union(List<DataSource<T>> otherDataSources) {
        return reduce(otherDataSources, Dataset::union);
    }

    default DataSource<T> peek(Consumer<DataSource<T>> peeker) {
        peeker.accept(this);
        return this;
    }

    // ==== UTILITIES ====

    default void show() {
        writeTo(new DataSinkShow<>());
    }

    default DataSource<Row> sql(String sql) {
        return transform(SqlTransformations.sql("source", sql));
    }

}
