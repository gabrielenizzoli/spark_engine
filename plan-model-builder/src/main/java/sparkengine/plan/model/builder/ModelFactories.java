package sparkengine.plan.model.builder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.catalog.SinkCatalog;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class ModelFactories {

    public static final ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    public static ComponentCatalog readComponentMapFromYaml(@Nonnull InputStreamSupplier inputStreamFactory) throws IOException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            var map = YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Component>>() {
            });
            return ComponentCatalog.ofMap(map);
        }
    }

    public static SinkCatalog readSinkMapFromYaml(@Nonnull InputStreamSupplier inputStreamFactory) throws IOException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            var map = YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Sink>>() {
            });
            return SinkCatalog.ofMap(map);
        }
    }

    public static Plan readPlanFromYaml(@Nonnull InputStreamSupplier inputStreamFactory) throws IOException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            return YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Plan>() {
            });
        }
    }

}
