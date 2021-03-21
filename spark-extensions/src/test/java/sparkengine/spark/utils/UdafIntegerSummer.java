package sparkengine.spark.utils;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import sparkengine.spark.sql.udf.UdafDefinition;

import javax.annotation.Nonnull;

public class UdafIntegerSummer extends UdafDefinition.UdafAggregator<Integer, Integer, Integer> {

    @Nonnull
    @Override
    public String getName() {
        return "summer";
    }

    @Override
    public Integer zero() {
        return 0;
    }

    @Override
    public Integer reduce(Integer input, Integer buffer) {
        return buffer + input;
    }

    @Override
    public Integer merge(Integer b1, Integer b2) {
        return b1 + b2;
    }

    @Override
    public Integer finish(Integer buffer) {
        return buffer;
    }

    @Override
    public Encoder<Integer> inputEncoder() {
        return Encoders.INT();
    }

    @Override
    public Encoder<Integer> bufferEncoder() {
        return Encoders.INT();
    }

    @Override
    public Encoder<Integer> outputEncoder() {
        return Encoders.INT();
    }

}
