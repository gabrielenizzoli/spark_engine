package dataengine.pipeline.datasetfactory;

import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

public interface DatasetFactory {

    @Nonnull
    <T> Dataset<T> buildDataset(String name) throws DatasetFactoryException;

}