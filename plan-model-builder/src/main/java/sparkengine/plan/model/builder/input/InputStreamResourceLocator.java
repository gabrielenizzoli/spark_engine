package sparkengine.plan.model.builder.input;

public interface InputStreamResourceLocator {

    InputStreamSupplier getInputStreamSupplier(String name);

}
