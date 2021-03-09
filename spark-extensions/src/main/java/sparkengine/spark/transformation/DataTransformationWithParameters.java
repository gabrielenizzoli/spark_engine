package sparkengine.spark.transformation;

import javax.annotation.Nullable;

public interface DataTransformationWithParameters<S, D, P> extends DataTransformationN<S, D> {

    Class<P> getParametersType();

    void setParameters(@Nullable P parameter);

}
