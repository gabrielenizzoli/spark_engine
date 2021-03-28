package sparkengine.spark.transformation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface DataTransformationWithParameters<P> {

    @Nonnull
    Class<P> getParametersType();

    void setParameters(@Nullable P parameter);

}
