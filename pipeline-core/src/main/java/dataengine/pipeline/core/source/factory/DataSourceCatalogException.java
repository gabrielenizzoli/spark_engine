package dataengine.pipeline.core.source.factory;

public class DataSourceCatalogException extends Exception {

    public DataSourceCatalogException(String str) {
        super(str);
    }

    public DataSourceCatalogException(String str, Throwable t) {
        super(str, t);
    }

    public static class ComponentNotFound extends DataSourceCatalogException {

        public ComponentNotFound(String str) {
            super(str);
        }

    }

    public static class InvalidPath extends DataSourceCatalogException {

        public InvalidPath(String str) {
            super(str);
        }

    }


}
