package dataengine.spark.transformation;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.storage.StorageLevel;

public class Transformations {

    public static <S, D> DataTransformation<S, D> map(MapFunction<S, D> map, Encoder<D> encoder) {
        return s -> s.map(map, encoder);
    }

    public static <S, D> DataTransformation<S, D> flatMap(FlatMapFunction<S, D> map, Encoder<D> encoder) {
        return s -> s.flatMap(map, encoder);
    }

    public static <S> DataTransformation<S, S> cache(StorageLevel storageLevel) {
        return s -> s.persist(storageLevel);
    }

    public static <S, D> DataTransformation<S, D> encodeAs(Encoder<D> encoder) {
        return s -> s.as(encoder);
    }

    public static <S> DataTransformation<S, Row> encodeAsRow() {
        return dataset -> {
            if (Row.class.isAssignableFrom(dataset.encoder().clsTag().runtimeClass()))
                return (Dataset<Row>)dataset;
            return dataset.toDF();
        };
    }

    public static <S> DataTransformation<S, S> store() {
        return new DataTransformation<S, S>() {

            Dataset<S> storedDataset = null;

            @Override
            public Dataset<S> apply(Dataset<S> dataset) {
                if (storedDataset != null)
                    return storedDataset;
                storedDataset = dataset;
                return storedDataset;
            }
        };
    }

}
