package sparkengine.spark.transformation;

import org.apache.spark.sql.Encoder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface DataTransformationWithEncoder<T> {

    void setEncoder(@Nullable Encoder<T> encoder);

}
