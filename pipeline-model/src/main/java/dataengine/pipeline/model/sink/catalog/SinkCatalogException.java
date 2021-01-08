package dataengine.pipeline.model.sink.catalog;

public class SinkCatalogException extends Exception {

    public SinkCatalogException(String str) {
        super(str);
    }

    public SinkCatalogException(String str, Throwable t) {
        super(str, t);
    }

}
