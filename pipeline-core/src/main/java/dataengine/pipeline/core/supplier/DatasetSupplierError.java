package dataengine.pipeline.core.supplier;

public class DatasetSupplierError extends RuntimeException {

    public DatasetSupplierError(String str) {
        super(str);
    }

    public DatasetSupplierError(String str, Throwable t) {
        super(str, t);
    }

}
