package dataengine.pipeline.model.builder.source.factory;

import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.description.encoder.TupleEncoder;
import dataengine.pipeline.model.description.encoder.ValueEncoder;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import javax.annotation.Nullable;

public class EncoderUtils {

    public static Encoder buildEncoder(@Nullable dataengine.pipeline.model.description.encoder.Encoder encoder)
            throws DataSourceFactoryException {
        if (encoder == null) {
            return null;
        } else if (encoder instanceof ValueEncoder) {
            switch (((ValueEncoder) encoder).getType()) {
                case BINARY:
                    return Encoders.BINARY();
                case BOOLEAN:
                    return Encoders.BOOLEAN();
                case BYTE:
                    return Encoders.BYTE();
                case DATE:
                    return Encoders.DATE();
                case DECIMAL:
                    return Encoders.DECIMAL();
                case DOUBLE:
                    return Encoders.DOUBLE();
                case FLOAT:
                    return Encoders.FLOAT();
                case INSTANT:
                    return Encoders.INSTANT();
                case INT:
                    return Encoders.INT();
                case LOCALDATE:
                    return Encoders.LOCALDATE();
                case LONG:
                    return Encoders.LONG();
                case SHORT:
                    return Encoders.SHORT();
                case STRING:
                    return Encoders.STRING();
                case TIMESTAMP:
                    return Encoders.TIMESTAMP();
            }
        } else if (encoder instanceof TupleEncoder) {
            TupleEncoder tupleEncoder = (TupleEncoder) encoder;
            Validate.listSize("tuple encoder", 2, 5);
            switch (tupleEncoder.getOf().size()) {
                case 2:
                    return Encoders.tuple(
                            buildEncoder(tupleEncoder.getOf().get(0)),
                            buildEncoder(tupleEncoder.getOf().get(1))
                    );
                case 3:
                    return Encoders.tuple(
                            buildEncoder(tupleEncoder.getOf().get(0)),
                            buildEncoder(tupleEncoder.getOf().get(1)),
                            buildEncoder(tupleEncoder.getOf().get(2))
                    );
                case 4:
                    return Encoders.tuple(
                            buildEncoder(tupleEncoder.getOf().get(0)),
                            buildEncoder(tupleEncoder.getOf().get(1)),
                            buildEncoder(tupleEncoder.getOf().get(2)),
                            buildEncoder(tupleEncoder.getOf().get(3))
                    );
                case 5:
                    return Encoders.tuple(
                            buildEncoder(tupleEncoder.getOf().get(0)),
                            buildEncoder(tupleEncoder.getOf().get(1)),
                            buildEncoder(tupleEncoder.getOf().get(2)),
                            buildEncoder(tupleEncoder.getOf().get(3)),
                            buildEncoder(tupleEncoder.getOf().get(4))
                    );
            }
        }
        throw new DataSourceFactoryException(encoder + " encoder not managed");
    }

}
