package dataengine.pipeline.model.builder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.catalog.ComponentCatalog;
import dataengine.pipeline.model.component.catalog.ComponentCatalogFromMap;
import dataengine.pipeline.model.pipeline.Pipelines;
import dataengine.pipeline.model.sink.Sink;
import dataengine.pipeline.model.sink.catalog.SinkCatalog;
import dataengine.pipeline.model.sink.catalog.SinkCatalogFromMap;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Supplier;

public class ModelReaders {

    public static final ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    public static ComponentCatalog readComponentMapFromYaml(@Nonnull Supplier<InputStream> inputStreamFactory) throws IOException {
        try (InputStream inputStream = inputStreamFactory.get()) {
            var map = YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Component>>() {
            });
            return ComponentCatalogFromMap.of(map);
        }
    }

    public static SinkCatalog readSinkMapFromYaml(@Nonnull Supplier<InputStream> inputStreamFactory) throws IOException {
        try (InputStream inputStream = inputStreamFactory.get()) {
            var map = YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Sink>>() {
            });
            return SinkCatalogFromMap.of(map);
        }
    }

    public static Pipelines readPipelinesFromYaml(@Nonnull Supplier<InputStream> inputStreamFactory) throws IOException {
        try (InputStream inputStream = inputStreamFactory.get()) {
            return YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Pipelines>() {
            });
        }
    }

}
