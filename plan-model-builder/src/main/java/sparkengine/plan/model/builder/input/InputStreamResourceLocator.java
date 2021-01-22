package sparkengine.plan.model.builder.input;

public interface InputStreamResourceLocator {

    InputStreamFactory getInputStreamFactory(String name);

}
