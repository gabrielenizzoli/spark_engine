package dataengine.pipeline.core.source.factory;

public class DataSourceFactoryException extends Exception {

    public DataSourceFactoryException(String str) {
        super(str);
    }

    public DataSourceFactoryException(String str, Throwable t) {
        super(str, t);
    }

}
