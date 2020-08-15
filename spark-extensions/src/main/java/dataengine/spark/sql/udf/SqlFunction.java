package dataengine.spark.sql.udf;

import javax.annotation.Nonnull;
import java.io.Serializable;

public interface SqlFunction extends Serializable {

    @Nonnull
    String getName();

}
