package com.hannea.tablestore;

public interface ITableStoreService {

	boolean exist(StoreTable table);

	boolean createTable(StoreTable table);

	boolean initClient();

	boolean putRow(StoreTableRow row);

	boolean putRowSelective(StoreTableRow row);
	
	boolean getRow(StoreTableRow row);
	
	boolean deleteRow(StoreTableRow row);
	
	boolean updateRow(StoreTableRow row);

	boolean updateRowSelective(StoreTableRow row);

	StoreTableRow initTable(String tableName, Object object);
}
