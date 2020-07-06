package dataengine.pipeline.model.description.source;

public interface ComponentCatalog {

    Component lookup(String componentName) throws ComponentCatalogException;

}
