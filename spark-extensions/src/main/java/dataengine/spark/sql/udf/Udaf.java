package dataengine.spark.sql.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;

import javax.annotation.Nonnull;

public interface Udaf<IN, OUT, BUF> extends SqlFunction {

    @Nonnull
    Encoder<IN> getInputEncoder();

    @Nonnull
    Aggregator<IN, OUT, BUF> getAggregator();

}
