package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.description.source.TransformationComponentWithMultipleInputs;
import dataengine.pipeline.model.description.source.TransformationComponentWithSingleInput;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class Validate {

    public interface Validator<T> {
        void accept(T t) throws DataSourceFactoryException;
    }

    public static Validator<List<?>> listSize(String msg, Integer min, Integer max) {
        return (in) -> {
            if (in == null || in.size() == 0)
                throw new DataSourceFactoryException(msg + " list is null or empty");
            if (min != null && in.size() < min)
                throw new DataSourceFactoryException(msg + " list size is less than " + min);
            if (max != null && in.size() > max)
                throw new DataSourceFactoryException(msg + " list size is more than " + max);
        };
    }

    public static <T> Validator<T> notNull(String msg) {
        return in -> {
            if (in == null)
                throw new DataSourceFactoryException(msg + " is null");
        };
    }

    public static Validator<String> notBlank(String msg) {
        return (in) -> {
            if (StringUtils.isBlank(in))
                throw new DataSourceFactoryException(msg + " is blank");
        };
    }

    /*
    public static DataSourceCatalog factoryOutput(DataSourceCatalog composer) {
        return (in) -> {
            try {
                return notNull(composer.apply(in), "datasource");
            } catch (Throwable t) {
                throw new DataFactoryException("datasource factory fails on name " + in, t);
            }
        };
    }
    */

    public static Validator<TransformationComponentWithMultipleInputs> multiInput(Integer min, Integer max) {
        return (in) -> {
            notNull("component").accept(in);
            listSize("components", min, max).accept(in.getUsing());
            Validator<String> notBlank = notBlank("component name");
            for (String name : in.getUsing())
                notBlank.accept(name);
        };
    }

    public static Validator<TransformationComponentWithSingleInput> singleInput() {
        return (in) -> {
            notNull( "step").accept(in);
            notNull("step name").accept(in.getUsing());
        };
    }

}
