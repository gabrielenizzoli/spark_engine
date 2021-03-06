package sparkengine.spark.sql.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;

import javax.annotation.Nonnull;

/**
 * A utility factory class that returns an Aggregator (the base class used by spark to implement a Udaf).
 * An aggregation is essentially a map-reduce job.
 *
 * @param <IN>  input type
 * @param <BUF> buffer that accumulates the input, according to some provided logic
 * @param <OUT> the output of the accumulation
 */
public interface Udaf<IN, BUF, OUT> extends SqlFunction {

    @Nonnull
    Aggregator<IN, BUF, OUT> getAggregator();

    Encoder<IN> inputEncoder();

    /**
     * A UDAF must return an Aggregator using the getAggregator method.
     * It is obviously possible to have the UDAF class also implements the Aggregator abstract class.
     * This class makes it easier to implement an aggregator this way, by simply extending the Aggregator base class.
     * <br><br>
     * Example:
     * <pre>
     * class IntegerSummer extends UdafAggregator&lt;Integer, Integer, Integer&gt; {
     *
     *    public String getName() { return "sum"; }
     *
     *    public Integer zero() { return 0; }
     *
     *    public Integer reduce(Integer in, Integer buf) { return buf + in; }
     *
     *    public Integer merge(Integer buf1, Integer buf22) { return buf1 + buf2; }
     *
     *    public Integer finish(Integer buffer) { return buffer; }
     *
     *    public Encoder&lt;Integer> inputEncoder() { return Encoders.INT(); }
     *
     *    public Encoder&lt;Integer> bufferEncoder() { return Encoders.INT(); }
     *
     *    public Encoder&lt;Integer> outputEncoder() { return Encoders.INT(); }
     *
     * }
     * </pre>
     */
    abstract class UdafAggregator<IN, BUF, OUT> extends Aggregator<IN, BUF, OUT> implements Udaf<IN, BUF, OUT> {

        @Nonnull
        @Override
        public final Aggregator<IN, BUF, OUT> getAggregator() {
            return this;
        }

    }
}
