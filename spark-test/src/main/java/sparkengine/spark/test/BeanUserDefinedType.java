package sparkengine.spark.test;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;
import scala.collection.JavaConverters;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class BeanUserDefinedType<T> extends UserDefinedType<T> {

    private final Class<T> type;
    private final StructType schema;
    private final ExpressionEncoder<T> expressionEncoder;

    private static Map<Class, UserDefinedType> UDTS = new ConcurrentHashMap<>();

    public static <T> UserDefinedType<T> getUDT(Class<T> t) {
        return UDTS.computeIfAbsent(t, type -> {
            var encoder = Encoders.bean(type);

            var attributes = JavaConverters.asScalaBuffer(JavaConverters.asJavaCollection(encoder.schema().toAttributes())
                    .stream()
                    .map(a -> (org.apache.spark.sql.catalyst.expressions.Attribute) a)
                    .collect(Collectors.toList()));

            return new BeanUserDefinedType<T>(
                    type,
                    encoder.schema(),
                    ((ExpressionEncoder<T>) encoder).resolveAndBind(attributes, SimpleAnalyzer$.MODULE$));
        });
    }

    private BeanUserDefinedType(Class<T> type,
                               StructType schema,
                               ExpressionEncoder<T> expressionEncoder) {
        this.type = type;
        this.schema = schema;
        this.expressionEncoder = expressionEncoder;
    }

    @Override
    public DataType sqlType() {
        return schema;
    }

    @Override
    public Object serialize(T obj) {
        return expressionEncoder.createSerializer().apply(obj);
    }

    @Override
    public T deserialize(Object datum) {
        return expressionEncoder.createDeserializer().apply((InternalRow) datum);
    }

    @Override
    public Class<T> userClass() {
        return type;
    }

}
