package dataengine.pipeline.core.source.factory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.core.source.impl.EmptyDataframeSource;
import dataengine.pipeline.core.source.impl.InlineJsonDataframeSource;
import dataengine.pipeline.model.description.source.component.InlineDataframeSource;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Value
public class InlineDataframeSourceFactory implements DataSourceFactory {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Nonnull
    InlineDataframeSource source;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {

        // normalize data
        List<String> json = Optional
                .ofNullable(source.getData())
                .map(mapsToJson())
                .orElse(Collections.emptyList());

        if (json.isEmpty()) {
            return EmptyDataframeSource.builder()
                    .schema(source.getSchema())
                    .build();
        }
        
        return InlineJsonDataframeSource.builder()
                .schema(source.getSchema())
                .json(json)
                .build();
    }

    @Nonnull
    private Function<List<Map<String, Object>>, List<String>> mapsToJson() {
        return maps -> maps
                .stream()
                .filter(Objects::nonNull)
                .filter(m -> !m.isEmpty())
                .map(mapToJson())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Nonnull
    private Function<Map<String, Object>, String> mapToJson() {
        return map -> {
            try {
                return OBJECT_MAPPER.writeValueAsString(map);
            } catch (JsonProcessingException e) {
                return null;
            }
        };
    }

}
