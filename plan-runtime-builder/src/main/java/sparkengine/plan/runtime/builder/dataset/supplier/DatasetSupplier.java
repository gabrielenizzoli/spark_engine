package sparkengine.plan.runtime.builder.dataset.supplier;

import org.apache.spark.sql.Dataset;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;

public interface DatasetSupplier<T> {

    Dataset<T> getDataset() throws DatasetFactoryException;

}
