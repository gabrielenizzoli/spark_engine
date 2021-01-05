package dataengine.pipeline.core.supplier.catalog;

public class DatasetSupplierCatalogException extends Exception {

    public DatasetSupplierCatalogException(String str) {
        super(str);
    }

    public DatasetSupplierCatalogException(String str, Throwable t) {
        super(str, t);
    }

    public static class ComponentNotFound extends DatasetSupplierCatalogException {

        public ComponentNotFound(String str) {
            super(str);
        }

    }

}
