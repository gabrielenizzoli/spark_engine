package sparkengine.plan.model.builder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import sparkengine.plan.model.builder.input.InputStreamFactory;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.catalog.SinkCatalog;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class ModelFactory {

    public static final ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    public static Component readComponentFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException, ModelFormatException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            return YAML_OBJECT_MAPPER.readValue(inputStream, Component.class);
        } catch (JsonProcessingException e) {
            throw new ModelFormatException("component format is not as expected", e);
        }
    }

    public static Map<String, Component> readComponentMapFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException, ModelFormatException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            return YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Component>>() {
            });
        } catch (JsonProcessingException e) {
            throw new ModelFormatException("component map format is not as expected", e);
        }
    }

    public static ComponentCatalog readComponentCatalogFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException, ModelFormatException {
        return ComponentCatalog.ofMap(readComponentMapFromYaml(inputStreamFactory));
    }

    public static Sink readSinkFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException, ModelFormatException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            return YAML_OBJECT_MAPPER.readValue(inputStream, Sink.class);
        } catch (JsonProcessingException e) {
            throw new ModelFormatException("sink format is not as expected", e);
        }
    }

    public static Map<String, Sink> readSinkMapFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException, ModelFormatException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            var map = YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Sink>>() {
            });
            return map;
        } catch (JsonProcessingException e) {
            throw new ModelFormatException("sink map format is not as expected", e);
        }
    }

    public static SinkCatalog readSinkCatalogFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException, ModelFormatException {
        return SinkCatalog.ofMap(readSinkMapFromYaml(inputStreamFactory));
    }

    public static Plan readPlanFromYaml(@Nonnull InputStreamFactory inputStreamFactory) throws IOException, ModelFormatException {
        try (InputStream inputStream = inputStreamFactory.getInputStream()) {
            return YAML_OBJECT_MAPPER.readValue(inputStream, new TypeReference<Plan>() {
            });
        } catch (JsonProcessingException e) {
            throw new ModelFormatException("component format is not as expected", e);
        }
    }

    public static void writePlanAsYaml(@Nonnull Plan plan, @Nonnull OutputStream outputStream) throws IOException, ModelFormatException {
        YAML_OBJECT_MAPPER.writeValue(outputStream, plan);
    }

}
