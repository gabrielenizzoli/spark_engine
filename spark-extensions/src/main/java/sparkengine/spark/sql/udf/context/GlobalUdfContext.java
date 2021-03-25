package sparkengine.spark.sql.udf.context;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

public class GlobalUdfContext {

    public static final UdfContext EMPTY_UDF_CONTEXT = new UdfContext() {

        @Override
        public void acc(String name, long value) {
        }

        @Override
        public String toString() {
            return "EMPTY";
        }

    };

    private static UdfContext GLOBAL_UDF_CONTEXT = null;

    private GlobalUdfContext() {

    }

    @Nonnull
    public static Optional<UdfContext> get() {
        return Optional.ofNullable(GLOBAL_UDF_CONTEXT);
    }

    public static void set(@Nullable UdfContext udfContext) {
        GLOBAL_UDF_CONTEXT = udfContext;
    }


}
