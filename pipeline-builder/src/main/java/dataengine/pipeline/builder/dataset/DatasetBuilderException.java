package dataengine.pipeline.builder.dataset;

public class DatasetBuilderException extends Exception {

    public DatasetBuilderException(String str) {
        super(str);
    }

    public DatasetBuilderException(String str, Throwable t) {
        super(str, t);
    }

    public static class ComponentNotFound extends DatasetBuilderException {

        public ComponentNotFound(String str) {
            super(str);
        }

    }

    public static class ComponentNotManaged extends DatasetBuilderException {

        public ComponentNotManaged(String str) {
            super(str);
        }

    }

    public static class CircularReference extends DatasetBuilderException {

        public CircularReference(String str) {
            super(str);
        }

    }

}
