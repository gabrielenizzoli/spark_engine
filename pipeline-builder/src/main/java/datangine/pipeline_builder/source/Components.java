package datangine.pipeline_builder.source;

import dataengine.model.pipeline.encoder.TupleEncoder;
import dataengine.model.pipeline.encoder.ValueEncoder;
import dataengine.model.pipeline.step.MultiInputStep;
import dataengine.model.pipeline.step.SingleInputStep;
import dataengine.model.pipeline.step.Source;
import dataengine.model.pipeline.step.source.SparkBatchSource;
import dataengine.model.pipeline.step.source.SparkSqlSource;
import dataengine.model.pipeline.step.source.SparkStreamSource;
import dataengine.model.pipeline.step.transformation.Encode;
import dataengine.model.pipeline.step.transformation.Sql;
import dataengine.model.pipeline.step.transformation.SqlMerge;
import dataengine.model.pipeline.step.transformation.Union;
import dataengine.pipeline.DataSource;
import dataengine.pipeline.source.SparkSource;
import dataengine.pipeline.transformation.Merge;
import dataengine.pipeline.transformation.SqlTransformations;
import datangine.pipeline_builder.PipelineBuilderException;
import datangine.pipeline_builder.validation.Validate;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public class Components {

    static DataSource buildDataSource(Source source) {
        if (source instanceof SparkSqlSource) {
            return buildSqlDataSource((SparkSqlSource) source);
        } else if (source instanceof SparkBatchSource) {
            return buildBatchDataSource((SparkBatchSource) source);
        } else if (source instanceof SparkStreamSource) {
            return buildStreamDataSource((SparkStreamSource) source);
        }
        return null;
    }

    private static DataSource buildSqlDataSource(SparkSqlSource source) {
        return dataengine.pipeline.source.SparkSqlSource.builder()
                .sql(source.getSql())
                .encoder(getEncoder(source.getAs()))
                .build();
    }

    private static DataSource buildBatchDataSource(@Nonnull SparkBatchSource source) {
        return SparkSource.builder()
                .format(source.getFormat())
                .options(source.getOptions())
                .encoder(getEncoder(source.getAs()))
                .type(SparkSource.SourceType.BATCH)
                .build();
    }

    private static DataSource buildStreamDataSource(@Nonnull SparkStreamSource source) {
        return SparkSource.builder()
                .format(source.getFormat())
                .options(source.getOptions())
                .encoder(getEncoder(source.getAs()))
                .type(SparkSource.SourceType.STREAM)
                .build();
    }

    public static Encoder getEncoder(@Nullable dataengine.model.pipeline.encoder.Encoder encoder) {
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
                            getEncoder(tupleEncoder.getOf().get(0)),
                            getEncoder(tupleEncoder.getOf().get(1))
                    );
                case 3:
                    return Encoders.tuple(
                            getEncoder(tupleEncoder.getOf().get(0)),
                            getEncoder(tupleEncoder.getOf().get(1)),
                            getEncoder(tupleEncoder.getOf().get(2))
                    );
                case 4:
                    return Encoders.tuple(
                            getEncoder(tupleEncoder.getOf().get(0)),
                            getEncoder(tupleEncoder.getOf().get(1)),
                            getEncoder(tupleEncoder.getOf().get(2)),
                            getEncoder(tupleEncoder.getOf().get(3))
                    );
                case 5:
                    return Encoders.tuple(
                            getEncoder(tupleEncoder.getOf().get(0)),
                            getEncoder(tupleEncoder.getOf().get(1)),
                            getEncoder(tupleEncoder.getOf().get(2)),
                            getEncoder(tupleEncoder.getOf().get(3)),
                            getEncoder(tupleEncoder.getOf().get(4))
                    );
            }
        }
        throw new PipelineBuilderException(encoder + " encoder not managed");
    }

    public static DataSource createSourceForSingleInputStep(@Nonnull SingleInputStep step,
                                                            @Nonnull DataSourceFactory dataSourceFactory) {

        Validate.singleInput().accept(step);
        dataSourceFactory = Validate.factoryOutput(dataSourceFactory);

        if (step instanceof Encode) {
            return dataSourceFactory
                    .apply(step.getUsing())
                    .encode(getEncoder(((Encode) step).getAs()));
        } else if (step instanceof Sql) {
            return dataSourceFactory
                    .apply(step.getUsing())
                    .transformation(SqlTransformations.sql(step.getUsing(), ((Sql) step).getSql()));
        }
        throw new PipelineBuilderException(step + " not managed");
    }

    public static DataSource createSourceForMultiInputStep(@Nonnull MultiInputStep step,
                                                           @Nonnull DataSourceFactory dataSourceFactory) {

        dataSourceFactory = Validate.factoryOutput(dataSourceFactory);

        if (step instanceof Union) {
            Validate.multiInput(2, null).accept(step);
            Union union = (Union) step;
            List<DataSource> dataSources = union.getUsing()
                    .stream()
                    .peek(Validate.notBlank("source name"))
                    .map(dataSourceFactory::apply)
                    .collect(Collectors.toList());
            return dataSources.get(0).union(dataSources.subList(1, dataSources.size()));
        } else if (step instanceof SqlMerge) {
            Validate.multiInput(2, 5).accept(step);
            SqlMerge sqlMerge = (SqlMerge) step;
            switch (sqlMerge.getUsing().size()) {
                case 2:
                    return Merge.mergeAll(
                            dataSourceFactory.apply(sqlMerge.getUsing().get(0)),
                            dataSourceFactory.apply(sqlMerge.getUsing().get(1)),
                            SqlTransformations.sqlMerge(
                                    sqlMerge.getUsing().get(0),
                                    sqlMerge.getUsing().get(1),
                                    sqlMerge.getSql())
                    );
                case 3:
                    return Merge.mergeAll(
                            dataSourceFactory.apply(sqlMerge.getUsing().get(0)),
                            dataSourceFactory.apply(sqlMerge.getUsing().get(1)),
                            dataSourceFactory.apply(sqlMerge.getUsing().get(2)),
                            SqlTransformations.sqlMerge(
                                    sqlMerge.getUsing().get(0),
                                    sqlMerge.getUsing().get(1),
                                    sqlMerge.getUsing().get(2),
                                    sqlMerge.getSql())
                    );
                case 4:
                    return Merge.mergeAll(
                            dataSourceFactory.apply(sqlMerge.getUsing().get(0)),
                            dataSourceFactory.apply(sqlMerge.getUsing().get(1)),
                            dataSourceFactory.apply(sqlMerge.getUsing().get(2)),
                            dataSourceFactory.apply(sqlMerge.getUsing().get(3)),
                            SqlTransformations.sqlMerge(
                                    sqlMerge.getUsing().get(0),
                                    sqlMerge.getUsing().get(1),
                                    sqlMerge.getUsing().get(2),
                                    sqlMerge.getUsing().get(3),
                                    sqlMerge.getSql())
                    );
                case 5:
                    return Merge.mergeAll(
                            dataSourceFactory.apply(sqlMerge.getUsing().get(0)),
                            dataSourceFactory.apply(sqlMerge.getUsing().get(1)),
                            dataSourceFactory.apply(sqlMerge.getUsing().get(2)),
                            dataSourceFactory.apply(sqlMerge.getUsing().get(3)),
                            dataSourceFactory.apply(sqlMerge.getUsing().get(4)),
                            SqlTransformations.sqlMerge(
                                    sqlMerge.getUsing().get(0),
                                    sqlMerge.getUsing().get(1),
                                    sqlMerge.getUsing().get(2),
                                    sqlMerge.getUsing().get(3),
                                    sqlMerge.getUsing().get(4),
                                    sqlMerge.getSql())
                    );
            }
        }
        throw new PipelineBuilderException(step + " not managed");
    }

}
