package dataengine.pipeline;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import dataengine.pipeline.core.consumer.factory.DatasetConsumerFactory;
import dataengine.pipeline.core.consumer.factory.DatasetConsumerFactoryException;
import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalog;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalogException;

public class Pipeline {

    String source;
    DatasetSupplierCatalog dataSourceComposer;
    DatasetConsumerFactory<?> datasetConsumerFactory;

    public void run() throws DatasetSupplierCatalogException, DatasetConsumerFactoryException {
        DatasetSupplier<?> datasetSupplier = dataSourceComposer.lookup(source);
        DatasetConsumer datasetConsumer = datasetConsumerFactory.build();
        datasetSupplier.writeTo(datasetConsumer);
    }

}
