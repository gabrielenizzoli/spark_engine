package dataengine.pipeline.runtime.builder.dataset.supplier;

import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import org.apache.spark.sql.Dataset;

public interface DatasetSupplier<T> {

    Dataset<T> provides() throws DatasetFactoryException;

}
