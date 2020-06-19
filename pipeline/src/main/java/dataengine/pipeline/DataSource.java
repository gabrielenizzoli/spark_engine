package dataengine.pipeline;

import dataengine.pipeline.transformation.Transformations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.storage.StorageLevel;

import java.util.List;
import java.util.function.Supplier;

public interface DataSource<T> extends Supplier<Dataset<T>> {

    default void write(DataSink<T> destination) {
        destination.accept(get());
    }

    default <D> DataSource<D> transformation(DataTransformation<T, D> mapper) {
        return () -> mapper.apply(get());
    }

    default DataSource<T> cache(StorageLevel storageLevel) {
        return transformation(Transformations.cache(storageLevel));
    }

    default <D> DataSource<D> encode(Encoder<D> encoder) {
        return transformation(Transformations.encode(encoder));
    }

    default <T2, D> DataSource<D> mergeWith(DataSource<T2> otherDataSources, Data2Transformation<T, T2, D> merger) {
        return () -> merger.apply(get(), otherDataSources.get());
    }

    default DataSource<T> reduce(List<DataSource<T>> otherDataSources, Data2Transformation<T, T, T> reducer) {
        return () -> otherDataSources.stream().map(Supplier::get).reduce(get(), reducer::apply);
    }

    default DataSource<T> union(List<DataSource<T>> otherDataSources) {
        return reduce(otherDataSources, Dataset::union);
    }

}
