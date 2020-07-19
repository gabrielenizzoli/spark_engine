package dataengine.pipeline.core.source.composer;

public class DataSourceComposerException extends Exception {

    public DataSourceComposerException(String str) {
        super(str);
    }

    public DataSourceComposerException(String str, Throwable t) {
        super(str, t);
    }

    public static class ComponentNotFound extends DataSourceComposerException {

        public ComponentNotFound(String str) {
            super(str);
        }

    }

}
