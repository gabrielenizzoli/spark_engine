package sparkengine.spark.sql.udf.context;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Map;

public interface UdfContext extends Serializable {

    UdfContext EMPTY_UDF_CONTEXT = new EmptyUdfContext();

    /**
     * Increase the accumulator value.
     * @param name  name of the accumulator
     * @param value value to increase (or decrease) the accumulator of.
     */
    void acc(String name, long value);

    default void acc(String name) {
        acc(name, 1L);
    }

}
