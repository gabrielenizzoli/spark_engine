package dataengine.pipeline.core.source;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.spark.transformation.DataTransformation;
import dataengine.spark.transformation.DataTransformation2;
import dataengine.spark.transformation.Transformations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.util.List;
import java.util.function.Supplier;

public interface DataSource<T> extends Supplier<Dataset<T>> {

    default void write(DataSink<T> destination) {
        destination.accept(get());
    }

    default <D> DataSource<D> transform(DataTransformation<T, D> mapper) {
        return DataSource1.of(this, mapper);
    }

    default DataSource<Row> toDataFrame() {
        return DataSource1.of(this, Transformations.dataFrame());
    }

    default DataSource<T> cache(StorageLevel storageLevel) {
        return transform(Transformations.cache(storageLevel));
    }

    default <D> DataSource<D> encode(Encoder<D> encoder) {
        return transform(Transformations.encode(encoder));
    }

    default <T2, D> DataSource<D> mergeWith(DataSource<T2> otherDataSource, DataTransformation2<T, T2, D> merger) {
        return DataSourceMerge.mergeAll(this, otherDataSource, merger);
    }

    default DataSource<T> reduce(List<DataSource<T>> otherDataSources, DataTransformation2<T, T, T> reducer) {
        return DataSourceMerge.reduce(this, otherDataSources, reducer);
    }

    default DataSource<T> union(List<DataSource<T>> otherDataSources) {
        return reduce(otherDataSources, Dataset::union);
    }

}
