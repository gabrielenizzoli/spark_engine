package sparkengine.delta.sharing.model;

import lombok.Value;

@Value
public class Table {

    TableName tableName;
    TableMetadata tableMetadata;

}
