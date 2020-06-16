package dataengine.pipelines;

import dataengine.pipelines.transformations.Transformation;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

import javax.annotation.Nonnull;
import java.util.List;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class DataPipe<T> {

    @Nonnull
    private final Dataset<T> dataset;

    public static <T> DataPipe<T> read(DataSource<T> source) {
        return new DataPipe<>(source.get());
    }

    public void write(DataSink<T> destination) {
        destination.accept(dataset);
    }

    public <D> DataPipe<D> transformation(DataTransformation<T, D> mapper) {
        return new DataPipe<>(mapper.apply(dataset));
    }

    public <D> DataPipe<D> encode(Encoder<D> encoder) {
        return transformation(Transformation.encode(encoder));
    }

    public <T2, D> DataPipe<D> mergeWith(DataPipe<T2> otherDataPipe, DataBiTransformation<T, T2, D> merger) {
        Dataset<D> mergedDataset = merger.apply(dataset, otherDataPipe.dataset);
        return new DataPipe<>(mergedDataset);
    }

    public DataPipe<T> reduce(List<DataPipe<T>> otherDataPipes, DataBiTransformation<T, T, T> reducer) {
        Dataset<T> reducedDataset = otherDataPipes.stream().map(o -> o.dataset).reduce(dataset, reducer::apply);
        return new DataPipe<>(reducedDataset);
    }

    public static <S1, S2, D> DataPipe<D> mergeAll(
            DataPipe<S1> pipe1, DataPipe<S2> pipe2,
            DataBiTransformation<S1, S2, D> merger) {
        return pipe1.mergeWith(pipe2, merger);
    }

    public static interface Data3Transformation<S1, S2, S3, D> {

        Dataset<D> apply(Dataset<S1> s1Dataset, Dataset<S2> s2Dataset, Dataset<S3> s3Dataset);

        default <D2> Data3Transformation<S1, S2, S3, D2> andThenEncode(Encoder<D2> encoder) {
            return (s1, s2, s3) -> Transformation.<D, D2>encode(encoder).apply(apply(s1, s2, s3));
        }

    }

    public static <S1, S2, S3, D> DataPipe<D> mergeAll(
            DataPipe<S1> pipe1, DataPipe<S2> pipe2, DataPipe<S3> pipe3,
            Data3Transformation<S1, S2, S3, D> merger) {
        Dataset<D> mergedDataset = merger.apply(pipe1.dataset, pipe2.dataset, pipe3.dataset);
        return new DataPipe<>(mergedDataset);
    }

}
