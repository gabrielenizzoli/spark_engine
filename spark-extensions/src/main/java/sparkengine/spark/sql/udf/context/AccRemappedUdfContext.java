package sparkengine.spark.sql.udf.context;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;

public class AccRemappedUdfContext extends WrapperUdfContext {

    @Nonnull
    private final Map<String, String> remapNames;

    public AccRemappedUdfContext(UdfContext udfContext,
                                 @Nullable Map<String, String> remapNames) {
        super(udfContext);
        this.remapNames = Optional.ofNullable(remapNames).filter(map -> !map.isEmpty()).orElse(Map.of());
    }

    @Override
    public void acc(String name, long value) {
        super.acc(remapNames.get(name), value);
    }

}
