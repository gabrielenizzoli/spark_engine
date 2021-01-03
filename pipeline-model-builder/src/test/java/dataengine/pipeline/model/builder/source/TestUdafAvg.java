package dataengine.pipeline.model.builder.source;

import dataengine.spark.sql.udf.Udaf;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;

import javax.annotation.Nonnull;

public class TestUdafAvg extends Udaf.UdafAggregator<Integer, Tuple2<Long, Integer>, Double> {

    @Nonnull
    @Override
    public String getName() {
        return "avvg";
    }

    @Override
    public Tuple2<Long, Integer> zero() {
        return new Tuple2<>(0L, 0);
    }

    @Override
    public Tuple2<Long, Integer> reduce(Tuple2<Long, Integer> b, Integer a) {
        if (a == null)
            return b;
        return new Tuple2<>(b._1() + a, b._2() + 1);
    }

    @Override
    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> b1, Tuple2<Long, Integer> b2) {
        return new Tuple2<>(b1._1() + b2._1(), b1._2() + b2._2());
    }

    @Override
    public Double finish(Tuple2<Long, Integer> reduction) {
        return (reduction._2() == 0) ? 0.0 : (reduction._1() * 1.0 / reduction._2() * 1.0);
    }

    @Override
    public Encoder<Integer> inputEncoder() {
        return Encoders.INT();
    }

    @Override
    public Encoder<Tuple2<Long, Integer>> bufferEncoder() {
        return Encoders.tuple(Encoders.LONG(), Encoders.INT());
    }

    @Override
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }
}
