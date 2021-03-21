package sparkengine.spark.sql.udf;

import org.apache.spark.broadcast.Broadcast;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

public class GlobalUdfContext {

    public static final UdfContext EMPTY_UDF_CONTEXT = new UdfContext() {
        @Override
        public String toString() {
            return "EMPTY";
        }
    };
    private static Broadcast<UdfContext> GLOBAL_UDF_CONTEXT = null;

    private GlobalUdfContext() {

    }

    @Nullable
    public static Broadcast<UdfContext> get() {
        return GLOBAL_UDF_CONTEXT;
    }

    public static void set(@Nonnull Broadcast<UdfContext> udfContext) {
        GLOBAL_UDF_CONTEXT = Objects.requireNonNull(udfContext);
    }


}
