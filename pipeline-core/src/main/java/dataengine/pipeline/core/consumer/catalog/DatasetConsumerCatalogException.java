package dataengine.pipeline.core.consumer.catalog;

public class DatasetConsumerCatalogException extends Exception {

    public DatasetConsumerCatalogException(String str) {
        super(str);
    }

    public DatasetConsumerCatalogException(String str, Throwable t) {
        super(str, t);
    }

    public static class ComponentNotFound extends DatasetConsumerCatalogException {

        public ComponentNotFound(String str) {
            super(str);
        }

    }

}
