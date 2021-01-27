package sparkengine.plan.runtime.builder.dataset.supplier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import sparkengine.plan.model.component.ComponentWithNoInput;
import sparkengine.plan.model.component.impl.BatchComponent;
import sparkengine.plan.model.component.impl.InlineComponent;
import sparkengine.plan.model.component.impl.StreamComponent;
import sparkengine.plan.runtime.builder.dataset.utils.EncoderUtils;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Value
@Builder
public class DatasetSupplierForComponentWithNoInput<T> implements DatasetSupplier<T> {

    @Nonnull
    SparkSession sparkSession;
    @Nonnull
    ComponentWithNoInput componentWithNoInput;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public Dataset<T> getDataset() throws DatasetFactoryException {
        if (componentWithNoInput instanceof BatchComponent) {
            return getBatchDataset((BatchComponent) componentWithNoInput).getDataset();
        }
        if (componentWithNoInput instanceof StreamComponent) {
            return getStreamDataset((StreamComponent) componentWithNoInput).getDataset();
        }
        if (componentWithNoInput instanceof InlineComponent) {
            return getInlineDataframeSourceDataset((InlineComponent) componentWithNoInput);
        }

        return null;
    }

    private DatasetSupplier<T> getBatchDataset(BatchComponent batchComponent) throws DatasetFactoryException {
        return DatasetSupplierForSpark.<T>builder()
                .sparkSession(sparkSession)
                .format(batchComponent.getFormat())
                .options(Optional.ofNullable(batchComponent.getOptions()).orElse(Map.of()))
                .encoder(EncoderUtils.buildEncoder(batchComponent.getEncodedAs()))
                .type(DatasetSupplierForSpark.SourceType.BATCH)
                .build();
    }

    private DatasetSupplier<T> getStreamDataset(StreamComponent streamComponent) throws DatasetFactoryException {
        return DatasetSupplierForSpark.<T>builder()
                .sparkSession(sparkSession)
                .format(streamComponent.getFormat())
                .options(Optional.ofNullable(streamComponent.getOptions()).orElse(Map.of()))
                .encoder(EncoderUtils.buildEncoder(streamComponent.getEncodedAs()))
                .type(DatasetSupplierForSpark.SourceType.STREAM)
                .build();
    }

    @Nullable
    private Dataset<T> getInlineDataframeSourceDataset(InlineComponent inlineComponent) throws DatasetFactoryException {
        List<String> json = parseJson(inlineComponent);
        var jsonDs = sparkSession.createDataset(json, Encoders.STRING());

        var reader = sparkSession.read();
        if (inlineComponent.getSchema() != null && !inlineComponent.getSchema().isBlank()) {
            var schema = StructType.fromDDL(inlineComponent.getSchema());
            reader = reader.schema(schema);
        }

        return (Dataset<T>) reader.json(jsonDs);
    }

    private static List<String> parseJson(InlineComponent inlineComponent) throws DatasetFactoryException {
        List<String> json = null;
        try {
            json = Optional.ofNullable(inlineComponent.getData()).orElse(List.of())
                    .stream()
                    .map(map -> {
                        try {
                            return OBJECT_MAPPER.writeValueAsString(map);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Throwable t) {
            throw new DatasetFactoryException("issue with inline dataset generation", t);
        }
        return json;
    }

}
