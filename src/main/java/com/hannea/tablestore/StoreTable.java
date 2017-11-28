package com.hannea.tablestore;

import java.util.List;

/**
 * Class
 *
 * @author wgm
 * @date 2017/11/03
 */
public class StoreTable {
    private String tableName;
    private List<PrimaryKeySchemaObject> primaryKeySchemaObjectList;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<PrimaryKeySchemaObject> getPrimaryKeySchemaObjectList() {
        return primaryKeySchemaObjectList;
    }

    public void setPrimaryKeySchemaObjectList(List<PrimaryKeySchemaObject> primaryKeySchemaObjectList) {
        this.primaryKeySchemaObjectList = primaryKeySchemaObjectList;
    }
}
