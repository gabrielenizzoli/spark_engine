package sparkengine.plan.runtime.builder.dataset.utils;

import org.codehaus.jackson.map.ObjectMapper;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.spark.transformation.DataTransformation;
import sparkengine.spark.transformation.DataTransformationN;
import sparkengine.spark.transformation.DataTransformationWithParameters;

import javax.annotation.Nonnull;
import java.util.Map;

public class TransformationUtils {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Nonnull
    public static <T> DataTransformation<Object, T> getDataTransformation(String typeName) throws DatasetFactoryException {
        try {
            return (DataTransformation<Object, T>) Class.forName(typeName).getDeclaredConstructor().newInstance();
        } catch (Throwable e) {
            throw new DatasetFactoryException(String.format("unable to instantiate transformation with class [%s]", typeName), e);
        }
    }

    @Nonnull
    public static <T> DataTransformationN<Object, T> getDataTransformationN(String typeName) throws DatasetFactoryException {
        try {
            return (DataTransformationN<Object, T>) Class.forName(typeName).getDeclaredConstructor().newInstance();
        } catch (Throwable e) {
            throw new DatasetFactoryException(String.format("unable to instantiate transformation with class [%s]", typeName), e);
        }
    }

    public static <T> void injectTransformationParameters(Object dataTransformation, Map<String, Object> params) throws DatasetFactoryException {
        if (dataTransformation instanceof DataTransformationWithParameters) {
            injectParameterObjectInDataTransformation((DataTransformationWithParameters) dataTransformation, params);
        } else if (params != null && !params.isEmpty()) {
            throw new DatasetFactoryException(String.format("transformation with class [%s] does not accepts parameters, but parameters provided [%s]",
                    dataTransformation.getClass().getName(),
                    params));
        }
    }

    public static <P> void injectParameterObjectInDataTransformation(DataTransformationWithParameters<P> dataTransformationWithParameters, Map<String, Object> params)
            throws DatasetFactoryException {

        var parameterType = dataTransformationWithParameters.getParametersType();

        if (parameterType == null) {
            throw new DatasetFactoryException(String.format("transformation class [%s] expects a parameter class, but no parameter class provided",
                    dataTransformationWithParameters.getClass().getName()));
        }

        if (params == null || params.isEmpty()) {
            throw new DatasetFactoryException(String.format("transformation class [%s] expects a parameter class [%s], but no parameters provided",
                    dataTransformationWithParameters.getClass().getName(),
                    parameterType.getName()));
        }

        try {
            var paramsObject = OBJECT_MAPPER.convertValue(params, parameterType);
            dataTransformationWithParameters.setParameters(paramsObject);
        } catch (Exception e) {
            throw new DatasetFactoryException(String.format("parameters for transformation class [%s] do not fit parameter class [%s]",
                    dataTransformationWithParameters.getClass().getName(),
                    parameterType.getName()),
                    e);
        }
    }

}
