package sparkengine.plan.runtime.datasetfactory;

import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.Map;

public interface DatasetFactory {

    static DatasetFactory ofMap(Map<String, Dataset> map) {
        return DatasetFactoryFromMap.of(map);
    }

    @Nonnull
    <T> Dataset<T> buildDataset(String name) throws DatasetFactoryException;

}
