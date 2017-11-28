package com.hannea.tablestore;

import java.util.Map;

/**
 * Class
 *
 * @author wgm
 * @date 2017/11/03
 */
public class StoreTableRow extends StoreTable{
    private Map<String, PrimaryKeyValueObject> primaryKeyValueObjectMap;
    private Map<String, ColumnValueObject> columnValueObjectMap;

    public Map<String, PrimaryKeyValueObject> getPrimaryKeyValueObjectMap() {
        return primaryKeyValueObjectMap;
    }

    public void setPrimaryKeyValueObjectMap(Map<String, PrimaryKeyValueObject> primaryKeyValueObjectMap) {
        this.primaryKeyValueObjectMap = primaryKeyValueObjectMap;
    }

    public Map<String, ColumnValueObject> getColumnValueObjectMap() {
        return columnValueObjectMap;
    }

    public void setColumnValueObjectMap(Map<String, ColumnValueObject> columnValueObjectMap) {
        this.columnValueObjectMap = columnValueObjectMap;
    }
}
