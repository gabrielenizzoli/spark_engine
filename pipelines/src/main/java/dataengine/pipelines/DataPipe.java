package dataengine.pipelines;

import dataengine.pipelines.transformations.Transformations;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.storage.StorageLevel;

import javax.annotation.Nonnull;
import java.util.List;

@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class DataPipe<T> {

    @Nonnull
    final Dataset<T> dataset;

    public static <T> DataPipe<T> read(DataSource<T> source) {
        return new DataPipe<>(source.get());
    }

    public void write(DataSink<T> destination) {
        destination.accept(dataset);
    }

    public <D> DataPipe<D> transformation(DataTransformation<T, D> mapper) {
        return new DataPipe<>(mapper.apply(dataset));
    }

    public DataPipe<T> cache(StorageLevel storageLevel) {
        return transformation(Transformations.cache(storageLevel));
    }

    public <D> DataPipe<D> encode(Encoder<D> encoder) {
        return transformation(Transformations.encode(encoder));
    }

    public <T2, D> DataPipe<D> mergeWith(DataPipe<T2> otherDataPipe, Data2Transformation<T, T2, D> merger) {
        Dataset<D> mergedDataset = merger.apply(dataset, otherDataPipe.dataset);
        return new DataPipe<>(mergedDataset);
    }

    public DataPipe<T> reduce(List<DataPipe<T>> otherDataPipes, Data2Transformation<T, T, T> reducer) {
        Dataset<T> reducedDataset = otherDataPipes.stream().map(o -> o.dataset).reduce(dataset, reducer::apply);
        return new DataPipe<>(reducedDataset);
    }

    public DataPipe<T> union(List<DataPipe<T>> otherDataPipes) {
        return reduce(otherDataPipes, Dataset::union);
    }

}
