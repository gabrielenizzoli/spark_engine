package dataengine.spark.sql.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;

import javax.annotation.Nonnull;

public abstract class UdafAggregator<IN, BUF, OUT> extends Aggregator<IN, BUF, OUT> implements Udaf<IN, BUF, OUT> {

    @Nonnull
    @Override
    public final Aggregator<IN, BUF, OUT> getAggregator() {
        return this;
    }

    @Override
    public final Encoder<IN> getInputEncoder() {
        return inputEncoder();
    }

    /**
     * This function replaces the getInputEncoder to match similar signatures for encoders in Aggregator class.
     */
    public abstract Encoder<IN> inputEncoder();

}
