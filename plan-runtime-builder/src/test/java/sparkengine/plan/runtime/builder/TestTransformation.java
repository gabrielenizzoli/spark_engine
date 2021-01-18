package sparkengine.plan.runtime.builder;

import sparkengine.spark.transformation.DataTransformationN;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class TestTransformation implements DataTransformationN<Row, Row> {

    @Override
    public Dataset<Row> apply(List<Dataset<Row>> datasets) {
        return datasets.stream().reduce(Dataset::union).orElseThrow(IllegalStateException::new);
    }

}
