package sparkengine.spark.sql.udf.context;

import javax.annotation.Nonnull;

public class WrapperUdfContext implements UdfContext {

    @Nonnull
    private final UdfContext udfContext;

    public WrapperUdfContext(UdfContext udfContext) {
        this.udfContext = udfContext;
    }

    @Override
    public void acc(String name, long value) {
        udfContext.acc(name, value);
    }

}
