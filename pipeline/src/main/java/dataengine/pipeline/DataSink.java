package dataengine.pipeline;

import org.apache.spark.sql.Dataset;

import java.util.function.Consumer;

public interface DataSink<T> extends Consumer<Dataset<T>> {

    default <S> DataSink<S> after(DataTransformation<S,T> tx) {
        return (s) -> this.accept(tx.apply(s));
    }

}
