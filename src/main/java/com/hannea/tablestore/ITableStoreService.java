package com.hannea.tablestore;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.filter.Filter;

import java.util.List;
import java.util.Map;

public interface ITableStoreService {

	boolean exist(StoreTable table);

	boolean createTable(StoreTable table);

	boolean initClient();

	boolean putRow(StoreTableRow row);

	boolean putRowSelective(StoreTableRow row);
	
	boolean getRow(StoreTableRow row);
	
	boolean deleteRow(StoreTableRow row);
	
	boolean updateRow(StoreTableRow row);

	boolean updateRow(String tablName, Map<String, PrimaryKeyValueObject> primaryKeyMap,List<Column> list);

	boolean updateRowSelective(StoreTableRow row);

	StoreTableRow initTable(String tableName, Object object);

	List<Row> batchGetRow(StoreTableRow row);

	List<Row> getRange(StoreTableRow storeTableRow, Filter filter, Map<String, PrimaryKeyValue> startPkValue, Map<String,PrimaryKeyValue> endPkValue);

	void atomicIncrement(String tableName, PrimaryKey primaryKey, String columnName, Object increment);

	boolean getAndSet(String tableName, PrimaryKey primaryKey, String columnName, Object increment);

	boolean updateIncrement(StoreTableRow storeTableRow,List<String> columnNameList);

	boolean updateIncrementAfterRowCheck(StoreTableRow storeTableRow,Row row,List<String> columnNameList);
}
