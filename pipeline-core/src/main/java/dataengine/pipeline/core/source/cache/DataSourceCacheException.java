package dataengine.pipeline.core.source.cache;

public class DataSourceCacheException extends Exception {

    public DataSourceCacheException(String str) {
        super(str);
    }

    public DataSourceCacheException(String str, Throwable t) {
        super(str, t);
    }

    public static class InvalidPath extends DataSourceCacheException {

        public InvalidPath(String str) {
            super(str);
        }

    }


}
