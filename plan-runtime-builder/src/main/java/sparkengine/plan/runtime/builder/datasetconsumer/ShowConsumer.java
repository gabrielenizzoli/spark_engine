package sparkengine.plan.runtime.builder.datasetconsumer;

import lombok.Builder;
import lombok.Value;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import scala.Console;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumer;

@Value
@Builder
@Log4j
public class ShowConsumer<T> implements DatasetConsumer<T> {

    int count;
    int truncate;

    @Override
    public void readFrom(Dataset<T> dataset) {
        dataset.show(count, truncate);
        Console.out().flush();
    }

}
