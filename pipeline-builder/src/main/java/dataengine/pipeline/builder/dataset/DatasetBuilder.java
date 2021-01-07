package dataengine.pipeline.builder.dataset;

import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

public interface DatasetBuilder {

    @Nonnull
    <T> Dataset<T> buildDataset(String name) throws DatasetBuilderException;

}
