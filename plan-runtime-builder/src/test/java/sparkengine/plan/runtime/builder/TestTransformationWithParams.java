package sparkengine.plan.runtime.builder;

import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import sparkengine.spark.transformation.DataTransformationWithParameters;

import javax.annotation.Nullable;
import java.util.List;

public class TestTransformationWithParams implements DataTransformationWithParameters<Row, Row, TestTransformationWithParams.Params> {

    private Params params;

    @Data
    public static class Params {
        boolean flag;
        String value;
    }

    @Override
    public Dataset<Row> apply(List<Dataset<Row>> datasets) {
        return datasets.stream().reduce(Dataset::union)
                .map(ds -> ds.withColumn(params.getValue(), functions.lit(params.isFlag())))
                .orElseThrow(IllegalStateException::new);
    }

    @Override
    public Class<Params> getParametersType() {
        return Params.class;
    }

    @Override
    public void setParameters(@Nullable Params parameter) {
        this.params = parameter;
    }

}
