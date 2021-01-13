package dataengine.pipeline.runtime.builder.dataset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dataengine.pipeline.model.component.SourceComponent;
import dataengine.pipeline.model.component.TransformationComponentWithMultipleInputs;
import dataengine.pipeline.model.component.TransformationComponentWithSingleInput;
import dataengine.pipeline.model.component.impl.*;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import dataengine.spark.transformation.DataTransformationN;
import dataengine.spark.transformation.Transformations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class Factories {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <T> Dataset<T> getSourceComponentDataset(@Nonnull SourceComponent sourceComponent) throws DatasetFactoryException {
        if (sourceComponent instanceof BatchComponent) {
            return getBatchDataset((BatchComponent) sourceComponent);
        }
        if (sourceComponent instanceof StreamComponent) {
            return getStreamDataset((StreamComponent) sourceComponent);
        }
        if (sourceComponent instanceof InlineComponent) {
            return getInlineDataframeSourceDataset((InlineComponent) sourceComponent);
        }

        return null;
    }

    public static <T> Dataset<T> getSingleInputComponent(TransformationComponentWithSingleInput singleInputComponent, Dataset<Object> inputDs) throws DatasetFactoryException {
        if (singleInputComponent instanceof EncodeComponent) {
            var encodeComponent = (EncodeComponent) singleInputComponent;
            Encoder<?> encoder = EncoderUtils.buildEncoder(encodeComponent.getEncodedAs());
            return (Dataset<T>) Transformations.encodeAs(encoder).apply(inputDs);
        }

        return null;
    }

    public static <T> Dataset<T> getMultiInputComponentDataset(TransformationComponentWithMultipleInputs multiInputComponent, List<Dataset<?>> parentDs) throws DatasetFactoryException {
        if (multiInputComponent instanceof UnionComponent) {
            return parentDs.stream()
                    .map(ds -> (Dataset<T>) ds)
                    .reduce(Dataset::union)
                    .orElseThrow(() -> new DatasetFactoryException("union can't be performed on an empty list of datasets"));
        }

        if (multiInputComponent instanceof SqlComponent) {
            var sqlComponent = (SqlComponent) multiInputComponent;
            return getSqlDataset(sqlComponent, parentDs);
        }

        if (multiInputComponent instanceof TransformComponent) {
            var txComponent = (TransformComponent) multiInputComponent;

            return getTransformDataset(parentDs, txComponent);
        }

        return null;
    }

    @Nullable
    private static <T> Dataset<T> getInlineDataframeSourceDataset(InlineComponent inlineComponent) throws DatasetFactoryException {
        List<String> json = parseJson(inlineComponent);
        var jsonDs = SparkSession.active().createDataset(json, Encoders.STRING());

        var reader = SparkSession.active().read();
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

    private static <T> Dataset<T> getTransformDataset(List<Dataset<?>> parentDs, TransformComponent txComponent) throws DatasetFactoryException {
        DataTransformationN<Object, T> dxTransformation = null;

        try {
            dxTransformation = (DataTransformationN<Object, T>) Class.forName(txComponent.getTransformWith()).getDeclaredConstructor().newInstance();
        } catch (Throwable e) {
            throw new DatasetFactoryException("unable to instantiate transformation with class: [" + txComponent.getTransformWith() + "]");
        }

        if (txComponent.getEncodedAs() != null) {
            var encoder = (Encoder<T>) EncoderUtils.buildEncoder(txComponent.getEncodedAs());
            dxTransformation = dxTransformation.andThenEncode(encoder);
        }

        return dxTransformation.apply(parentDs.stream().map(m -> (Dataset<Object>) m).collect(Collectors.toList()));
    }

    private static <T> Dataset<T> getSqlDataset(SqlComponent sqlComponent, List<Dataset<?>> parentDs) throws DatasetFactoryException {
        var sqlFunctions = UdfUtils.buildSqlFunctionCollection(sqlComponent.getUdfs());
        var rowEncoder = Transformations.encodeAsRow();
        var parentDf = parentDs.stream().map(ds -> (Dataset<Object>) ds).map(rowEncoder::apply).collect(Collectors.toList());

        var tx = Transformations.sql(sqlComponent.getUsing(), sqlComponent.getSql(), sqlFunctions);
        if (sqlComponent.getEncodedAs() != null) {
            var encode = EncoderUtils.buildEncoder(sqlComponent.getEncodedAs());
            tx = tx.andThenEncode(encode);
        }

        return (Dataset<T>) tx.apply(parentDf);
    }

    private static <T> Dataset<T> getStreamDataset(StreamComponent streamComponent) throws DatasetFactoryException {
        return (Dataset<T>) SparkSourceFactory.<T>builder()
                .format(streamComponent.getFormat())
                .options(streamComponent.getOptions())
                .encoder(EncoderUtils.buildEncoder(streamComponent.getEncodedAs()))
                .type(SparkSourceFactory.SourceType.STREAM)
                .build()
                .buildDataset();
    }

    private static <T> Dataset<T> getBatchDataset(BatchComponent batchComponent) throws DatasetFactoryException {
        return (Dataset<T>) SparkSourceFactory.<T>builder()
                .format(batchComponent.getFormat())
                .options(batchComponent.getOptions())
                .encoder(EncoderUtils.buildEncoder(batchComponent.getEncodedAs()))
                .type(SparkSourceFactory.SourceType.BATCH)
                .build()
                .buildDataset();
    }

}
