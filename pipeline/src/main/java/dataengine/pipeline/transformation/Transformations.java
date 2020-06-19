package dataengine.pipeline.transformation;

import dataengine.pipeline.DataTransformation;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.storage.StorageLevel;

public class Transformations {

    public static <S, D> DataTransformation<S, D> map(MapFunction<S, D> map, Encoder<D> encoder) {
        return s -> s.map(map, encoder);
    }

    public static <S, D> DataTransformation<S, D> flatMap(FlatMapFunction<S, D> map, Encoder<D> encoder) {
        return s -> s.flatMap(map, encoder);
    }

    public static <S, D> DataTransformation<S, D> encode(Encoder<D> encoder) {
        return s -> s.as(encoder);
    }

    public static <S> DataTransformation<S, S> cache(StorageLevel storageLevel) {
        return s -> s.persist(storageLevel);
    }

}
