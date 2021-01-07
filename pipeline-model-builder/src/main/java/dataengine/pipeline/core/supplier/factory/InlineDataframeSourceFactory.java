package dataengine.pipeline.core.supplier.factory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.impl.EmptyDataframeSupplier;
import dataengine.pipeline.core.supplier.impl.InlineJsonSupplier;
import dataengine.pipeline.model.source.component.InlineSource;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Value
public class InlineDataframeSourceFactory implements DatasetSupplierFactory {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Nonnull
    InlineSource source;

    @Override
    public DatasetSupplier<?> build() throws DatasetSupplierFactoryException {

        // normalize data
        List<String> json = Optional
                .ofNullable(source.getData())
                .map(mapsToJson())
                .orElse(Collections.emptyList());

        if (json.isEmpty()) {
            return EmptyDataframeSupplier.builder()
                    .schema(source.getSchema())
                    .build();
        }
        
        return InlineJsonSupplier.builder()
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
