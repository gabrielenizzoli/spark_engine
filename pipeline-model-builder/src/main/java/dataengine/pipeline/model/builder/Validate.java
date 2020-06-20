package dataengine.pipeline.model.builder;

import dataengine.pipeline.model.pipeline.sink.Sink;
import dataengine.pipeline.model.pipeline.step.MultiInputStep;
import dataengine.pipeline.model.pipeline.step.SingleInputStep;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.function.Consumer;

public class Validate {

    public static Consumer<List> listSize(String msg, Integer min, Integer max) {
        return (in) -> {
            if (in == null || in.size() == 0)
                throw new ModelBuilderException(msg + " list is null or empty");
            if (min != null && in.size() < min)
                throw new ModelBuilderException(msg + " list size is less than " + min);
            if (max != null && in.size() > max)
                throw new ModelBuilderException(msg + " list size is more than " + max);
        };
    }

    public static <T> Consumer<T> notNull(String msg) {
        return (in) -> {
            if (in == null)
                throw new ModelBuilderException(msg + " is null");
        };
    }

    public static Consumer<String> notBlank(String msg) {
        return (in) -> {
            if (StringUtils.isBlank(in))
                throw new ModelBuilderException(msg + " is blank");
        };
    }

    public static DataSourceFactory factoryOutput(DataSourceFactory factory) {
        return (in) -> {
            try {
                DataSource ds = factory.apply(in);
                notNull("datasource").accept(ds);
                return ds;
            } catch (Throwable t) {
                throw new ModelBuilderException("datasource factory fails on name " + in, t);
            }
        };
    }

    public static Consumer<MultiInputStep> multiInput(Integer min, Integer max) {
        return (in) -> {
            notNull("step").accept(in);
            listSize("step name", min, max).accept(in.getUsing());
            in.getUsing().forEach(notNull("step name"));
        };
    }

    public static Consumer<SingleInputStep> singleInput() {
        return (in) -> {
            notNull("step").accept(in);
            notNull("step name").accept(in.getUsing());
        };
    }

    public static Consumer<Sink> sink() {
        return (in) -> {
            notNull("sink").accept(in);
            notNull("step name").accept(in.getUsing());
        };
    }

}
