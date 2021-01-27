package sparkengine.plan.runtime.builder.dataset.supplier;

import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import org.apache.spark.sql.Dataset;

public interface DatasetSupplier<T> {

    Dataset<T> getDataset() throws DatasetFactoryException;

}
