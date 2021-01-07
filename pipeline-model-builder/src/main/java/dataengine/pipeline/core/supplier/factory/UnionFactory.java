package dataengine.pipeline.core.supplier.factory;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalog;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalogException;
import dataengine.pipeline.model.source.component.Union;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

@Value
public class UnionFactory implements DatasetSupplierFactory {

    @Nonnull
    Union union;
    @Nonnull
    DatasetSupplierCatalog DatasetSupplierCatalog;

    @Override
    public DatasetSupplier<?> build() throws DatasetSupplierFactoryException {
        Validate.multiInput(2, null).accept(union);

        List<DatasetSupplier> datasetSuppliers = new ArrayList<>(union.getUsing().size());
        for (String name : union.getUsing()) {
            Validate.notBlank("source name").accept(name);
            try {
                datasetSuppliers.add(DatasetSupplierCatalog.lookup(name));
            } catch (DatasetSupplierCatalogException e) {
                throw new DatasetSupplierFactoryException("can't locate datasource with name " + name, e);
            }
        }
        return datasetSuppliers.get(0).union(datasetSuppliers.subList(1, datasetSuppliers.size()));
    }

}
