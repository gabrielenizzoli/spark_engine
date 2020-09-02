package dataengine.pipeline.core.sink.composer;

public class DataSinkComposerException extends Exception {

    public DataSinkComposerException(String str) {
        super(str);
    }

    public DataSinkComposerException(String str, Throwable t) {
        super(str, t);
    }

    public static class ComponentNotFound extends DataSinkComposerException {

        public ComponentNotFound(String str) {
            super(str);
        }

    }

}
