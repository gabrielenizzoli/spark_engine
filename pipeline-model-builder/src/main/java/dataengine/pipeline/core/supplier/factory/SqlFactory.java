package dataengine.pipeline.core.supplier.factory;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.impl.DatasetSupplierMerge;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalog;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalogException;
import dataengine.pipeline.core.supplier.utils.EncoderUtils;
import dataengine.pipeline.core.supplier.utils.UdfUtils;
import dataengine.pipeline.model.source.component.Sql;
import dataengine.spark.transformation.Transformations;
import lombok.Value;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

@Value
public class SqlFactory implements DatasetSupplierFactory {

    @Nonnull
    Sql sql;
    @Nonnull
    DatasetSupplierCatalog DatasetSupplierCatalog;

    @Override
    public DatasetSupplier<?> build() throws DatasetSupplierFactoryException {
        DatasetSupplier<?> datasetSupplier = getDataSource();
        if (sql.getEncodedAs() == null)
            return datasetSupplier;
        return datasetSupplier.encodeAs(EncoderUtils.buildEncoder(sql.getEncodedAs()));
    }

    private DatasetSupplier<?> getDataSource() throws DatasetSupplierFactoryException {

        Validate.multiInput(1, null).accept(sql);

        var sqlFunctions = UdfUtils.buildSqlFunctionCollection(sql.getUdfs());

        return DatasetSupplierMerge.mergeAll(
                getDataSources(),
                Transformations.sql(sql.getUsing(), sql.getSql(), sqlFunctions)
        );
    }

    @Nonnull
    private List<DatasetSupplier<Row>> getDataSources() throws DatasetSupplierFactoryException {
        List<DatasetSupplier<Row>> datasetSuppliers = new ArrayList<>(sql.getUsing().size());
        for (int i = 0; i < sql.getUsing().size(); i++)
            datasetSuppliers.add(lookupDataSource(sql.getUsing().get(i)).transform(Transformations.encodeAsRow()));
        return datasetSuppliers;
    }

    private DatasetSupplier<?> lookupDataSource(String name) throws DatasetSupplierFactoryException {
        try {
            return DatasetSupplierCatalog.lookup(name);
        } catch (DatasetSupplierCatalogException e) {
            throw new DatasetSupplierFactoryException("can't locate datasource with name " + name, e);
        }
    }

}
