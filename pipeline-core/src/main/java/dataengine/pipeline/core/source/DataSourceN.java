package dataengine.pipeline.core.source;

import dataengine.spark.transformation.DataTransformationN;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Value
@AllArgsConstructor(staticName = "of")
@Builder
public class DataSourceN<S, D> implements DataSource<D> {

    @Nonnull
    List<DataSource<S>> parentDataSources;
    @Nonnull
    DataTransformationN<S, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(parentDataSources.stream().map(Supplier::get).collect(Collectors.toList()));
    }

}
