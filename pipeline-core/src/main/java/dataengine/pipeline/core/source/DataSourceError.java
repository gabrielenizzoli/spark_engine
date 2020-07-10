package dataengine.pipeline.core.source;

public class DataSourceError extends RuntimeException {

    public DataSourceError(String str) {
        super(str);
    }

    public DataSourceError(String str, Throwable t) {
        super(str, t);
    }

}
