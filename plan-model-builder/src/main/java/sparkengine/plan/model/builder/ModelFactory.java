package sparkengine.plan.model.builder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.builder.input.InputStreamFactory;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.catalog.SinkCatalog;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class ModelFactory {

    public static final ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    public static Component readComponentFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            return YAML_OBJECT_MAPPER.readValue(inputStream, Component.class);
        }
    }

    public static Map<String, Component> readComponentMapFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            return YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Component>>() {
            });
        }
    }

    public static ComponentCatalog readComponentCatalogFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException {
        return ComponentCatalog.ofMap(readComponentMapFromYaml(inputStreamFactory));
    }

    public static SinkCatalog readSinkMapFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            var map = YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Sink>>() {
            });
            return SinkCatalog.ofMap(map);
        }
    }

    public static Plan readPlanFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            return YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Plan>() {
            });
        }
    }

}