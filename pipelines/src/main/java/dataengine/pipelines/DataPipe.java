package dataengine.pipelines;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.Collection;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class DataPipe<T> {

    @Nonnull
    Dataset<T> dataset;

    public static <T> DataPipe<T> read(DataSource<T> source) {
        return new DataPipe<>(source.get());
    }

    public <D> DataPipe<D> transformation(DataTransformation<T, D> mapper) {
        return new DataPipe<>(mapper.apply(dataset));
    }

    public <T2, D> DataPipe<D> merge(DataPipe<T2> otherDataPipe, DataBiTransformation<T, T2, D> merger) {
        Dataset<D> mergedDataset = merger.apply(dataset, otherDataPipe.dataset);
        return new DataPipe<>(mergedDataset);
    }

    public DataPipe<T> reduce(DataBiTransformation<T, T, T> reducer, Collection<DataPipe<T>> otherDataPipes) {
        Dataset<T> reducedDataset = otherDataPipes.stream().map(o -> o.dataset).reduce(dataset, reducer::apply);
        return new DataPipe<>(reducedDataset);
    }

    public void write(DataSink<T> destination) {
        destination.accept(dataset);
    }

}
