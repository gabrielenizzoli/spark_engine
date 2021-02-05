package sparkengine.spark.transformation;

import org.apache.spark.sql.types.StructType;

public class TransformationException extends RuntimeException {

    public TransformationException(String str) {
        super(str);
    }

    public TransformationException(String str, Throwable t) {
        super(str, t);
    }

    public static class InvalidSchema extends TransformationException {

        private final StructType expectedSchema;

        public InvalidSchema(String str, StructType expectedSchema) {
            super(str);
            this.expectedSchema = expectedSchema;
        }

    }

}
