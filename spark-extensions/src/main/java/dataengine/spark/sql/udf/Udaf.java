package dataengine.spark.sql.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;

import javax.annotation.Nonnull;

public interface Udaf<IN, BUF, OUT> extends SqlFunction {

    Encoder<IN> getInputEncoder();

    @Nonnull
    Aggregator<IN, BUF, OUT> getAggregator();

}
