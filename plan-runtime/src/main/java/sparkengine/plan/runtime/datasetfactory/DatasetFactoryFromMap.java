package sparkengine.plan.runtime.datasetfactory;

import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

@Value(staticConstructor = "of")
public class DatasetFactoryFromMap implements DatasetFactory {

    @Nonnull
    Map<String, Dataset> datasets;

    @Nonnull
    @Override
    public <T> Dataset<T> buildDataset(String name) throws DatasetFactoryException {
        return Optional.ofNullable(datasets.get(name))
                .map(dataset -> (Dataset<T>)dataset)
                .orElseThrow(() -> new DatasetFactoryException("dataset not found with name " + name));
    }

}
