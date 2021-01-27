package sparkengine.plan.model.builder;

public class ModelFormatException extends Exception {

    public ModelFormatException(String str) {
        super(str);
    }

    public ModelFormatException(String str, Throwable t) {
        super(str, t);
    }

}
