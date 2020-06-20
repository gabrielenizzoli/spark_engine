package dataengine.pipeline.model.builder;

public class ModelBuilderException extends RuntimeException {

    public ModelBuilderException(String str) {
        super(str);
    }

    public ModelBuilderException(String str, Throwable t) {
        super(str, t);
    }

}
