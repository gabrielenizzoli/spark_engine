package dataengine.pipeline.core.supplier.factory;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.impl.SparkSqlSource;
import dataengine.pipeline.core.supplier.utils.EncoderUtils;
import dataengine.pipeline.core.supplier.utils.UdfUtils;
import dataengine.pipeline.model.description.source.component.SqlSource;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class SqlSourceFactory implements DatasetSupplierFactory {

    @Nonnull
    SqlSource source;

    @Override
    public DatasetSupplier<?> build() throws DatasetSupplierFactoryException {
        return SparkSqlSource.builder()
                .sql(source.getSql())
                .sqlFunctions(UdfUtils.buildSqlFunctionCollection(source.getUdfs()))
                .encoder(EncoderUtils.buildEncoder(source.getEncodedAs()))
                .build();
    }

}
