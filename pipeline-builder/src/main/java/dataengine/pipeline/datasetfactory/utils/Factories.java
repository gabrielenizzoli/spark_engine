package dataengine.pipeline.datasetfactory.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dataengine.pipeline.datasetfactory.DatasetFactoryException;
import dataengine.pipeline.model.source.SourceComponent;
import dataengine.pipeline.model.source.TransformationComponentWithMultipleInputs;
import dataengine.pipeline.model.source.TransformationComponentWithSingleInput;
import dataengine.pipeline.model.source.component.*;
import dataengine.pipeline.utils.EncoderUtils;
import dataengine.pipeline.utils.UdfUtils;
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
        if (sourceComponent instanceof BatchSource) {
            return getBatchDataset((BatchSource) sourceComponent);
        }
        if (sourceComponent instanceof StreamSource) {
            return getStreamDataset((StreamSource) sourceComponent);
        }
        if (sourceComponent instanceof InlineSource) {
            return getInlineDataframeSourceDataset((InlineSource) sourceComponent);
        }

        return null;
    }

    public static <T> Dataset<T> getSingleInputComponent(TransformationComponentWithSingleInput singleInputComponent, Dataset<Object> inputDs) throws DatasetFactoryException {
        if (singleInputComponent instanceof Encode) {
            var encodeComponent = (Encode) singleInputComponent;
            Encoder<?> encoder = EncoderUtils.buildEncoder(encodeComponent.getEncodedAs());
            return (Dataset<T>) Transformations.encodeAs(encoder).apply(inputDs);
        }

        return null;
    }

    public static <T> Dataset<T> getMultiInputComponentDataset(TransformationComponentWithMultipleInputs multiInputComponent, List<Dataset<?>> parentDs) throws DatasetFactoryException {
        if (multiInputComponent instanceof Union) {
            return parentDs.stream()
                    .map(ds -> (Dataset<T>) ds)
                    .reduce(Dataset::union)
                    .orElseThrow(() -> new DatasetFactoryException("union can't be performed on an empty list of datasets"));
        }

        if (multiInputComponent instanceof Sql) {
            var sqlComponent = (Sql) multiInputComponent;
            return getSqlDataset(sqlComponent, parentDs);
        }

        if (multiInputComponent instanceof Transform) {
            var txComponent = (Transform) multiInputComponent;

            return getTransformDataset(parentDs, txComponent);
        }

        return null;
    }

    @Nullable
    private static <T> Dataset<T> getInlineDataframeSourceDataset(InlineSource inlineSource) {
        List<String> json = Optional.ofNullable(inlineSource.getData()).orElse(List.of())
                .stream()
                .map(map -> {
                    try {
                        return OBJECT_MAPPER.writeValueAsString(map);
                    } catch (JsonProcessingException e) {
                        // TODO error
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        var schema = StructType.fromDDL(inlineSource.getSchema());
        var reader = SparkSession.active().read().schema(schema);
        var jsonDs = SparkSession.active().createDataset(json, Encoders.STRING());

        return (Dataset<T>) reader.json(jsonDs);
    }

    private static <T> Dataset<T> getTransformDataset(List<Dataset<?>> parentDs, Transform txComponent) throws DatasetFactoryException {
        DataTransformationN<Object, T> dxTransformation = null;

        try {
            dxTransformation = (DataTransformationN<Object, T>) Class.forName(txComponent.getWith()).getDeclaredConstructor().newInstance();
        } catch (Throwable e) {
            throw new DatasetFactoryException("unable to instantiate transformation with class: [" + txComponent.getWith() + "]");
        }

        if (txComponent.getEncodedAs() != null) {
            var encoder = (Encoder<T>) EncoderUtils.buildEncoder(txComponent.getEncodedAs());
            dxTransformation = dxTransformation.andThenEncode(encoder);
        }

        return dxTransformation.apply(parentDs.stream().map(m -> (Dataset<Object>) m).collect(Collectors.toList()));
    }

    private static <T> Dataset<T> getSqlDataset(Sql sqlComponent, List<Dataset<?>> parentDs) throws DatasetFactoryException {
        var sqlFunctions = UdfUtils.buildSqlFunctionCollection(sqlComponent.getUdfs());
        var rowEncoder = Transformations.encodeAsRow();
        var parentDf = parentDs.stream().map(ds -> (Dataset<Object>) ds).map(rowEncoder::apply).collect(Collectors.toList());
        return (Dataset<T>) Transformations.sql(sqlComponent.getUsing(), sqlComponent.getSql(), sqlFunctions).apply(parentDf);
    }

    private static <T> Dataset<T> getStreamDataset(StreamSource streamSource) throws DatasetFactoryException {
        return (Dataset<T>) SparkSourceFactory.<T>builder()
                .format(streamSource.getFormat())
                .options(streamSource.getOptions())
                .encoder(EncoderUtils.buildEncoder(streamSource.getEncodedAs()))
                .type(SparkSourceFactory.SourceType.STREAM)
                .build()
                .buildDataset();
    }

    private static <T> Dataset<T> getBatchDataset(BatchSource batchSource) throws DatasetFactoryException {
        return (Dataset<T>) SparkSourceFactory.<T>builder()
                .format(batchSource.getFormat())
                .options(batchSource.getOptions())
                .encoder(EncoderUtils.buildEncoder(batchSource.getEncodedAs()))
                .type(SparkSourceFactory.SourceType.BATCH)
                .build()
                .buildDataset();
    }

}
