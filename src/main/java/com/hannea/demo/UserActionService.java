package com.hannea.demo;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.hannea.demo.instance.ActionTableStoreServiceImpl;
import com.hannea.tablestore.StoreTableRow;

import java.util.*;

/**
 * Class
 *
 * @author wgm
 * @date 2017/11/28
 */
public class UserActionService {

    //理论上和UserAction中注解值一样，但是会优先使用这里面的，如果这个设置空，则会使用注解中的
    private static final String actionTableName = "table_name_test";

    public static void main(String[] args) {
        UserAction userAction = new UserAction();
        userAction.setHashUserId("hashId");
        userAction.setUserId(1);
        userAction.setUserType(1);
        userAction.setActionType(1);
        userAction.setTimes(System.currentTimeMillis());
        ActionTableStoreServiceImpl actionTableStoreService = new ActionTableStoreServiceImpl();
        StoreTableRow row = actionTableStoreService.initTable(actionTableName,userAction);
        if(row == null){
            System.out.println("init error");
            return;
        }
        actionTableStoreService.putRow(row);
    }


    public static void getUserAction(){
        ActionTableStoreServiceImpl actionTableStoreService = new ActionTableStoreServiceImpl();
        Map<String,PrimaryKeyValue> startPkValue = new LinkedHashMap<>();
        Map<String, PrimaryKeyValue > endPkValue = new LinkedHashMap<>();
        startPkValue.put("hashUserId", PrimaryKeyValue.INF_MIN);
        startPkValue.put("userId", PrimaryKeyValue.INF_MIN);
        startPkValue.put("userType", PrimaryKeyValue.INF_MIN);

        endPkValue.put("hashUserId", PrimaryKeyValue.INF_MAX);
        endPkValue.put("userId", PrimaryKeyValue.INF_MAX);
        endPkValue.put("userType", PrimaryKeyValue.INF_MAX);

        StoreTableRow storeTableRow1 = new StoreTableRow();
        storeTableRow1.setTableName(actionTableName);
        SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter("hashUserId",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromString("hashId"));
        SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter("userId",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(1));
        CompositeColumnValueFilter compositeColumnValueFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.AND);
        compositeColumnValueFilter.addFilter(singleColumnValueFilter1);
        compositeColumnValueFilter.addFilter(singleColumnValueFilter2);
        List<Row> rows = actionTableStoreService.getRange(storeTableRow1, compositeColumnValueFilter, startPkValue, endPkValue);
        if(rows == null) {
            System.out.println("null");
        }
        List<UserAction> list = new ArrayList<>();
        for(Row row : rows){
            UserAction stat = new UserAction();
            Map<String,Object> map = new HashMap<>();
            for(Map.Entry<String,PrimaryKeyColumn> entry : row.getPrimaryKey().getPrimaryKeyColumnsMap().entrySet()){
                map.put(entry.getKey(),entry.getValue().getValue());
            }
            for(Column column : row.getColumns()){
                map.put(column.getName(),column.getValue());
            }
            try {
                actionTableStoreService.setFieldValue(map,stat);
                list.add(stat);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static void doUpdateRow(){
        UserAction userAction = new UserAction();
        userAction.setHashUserId("hashId");
        userAction.setUserId(1);
        userAction.setActionType(1);
        userAction.setTimes(System.currentTimeMillis());
        ActionTableStoreServiceImpl actionTableStoreService = new ActionTableStoreServiceImpl();
        StoreTableRow storeTableRow = actionTableStoreService.initTable(actionTableName,userAction);
        //设置需要更新的列和值
        List<Column> updatelist = new ArrayList<>();
        updatelist.add(new Column("times",ColumnValue.fromLong(userAction.getTimes())));
        actionTableStoreService.updateRow(actionTableName,storeTableRow.getPrimaryKeyValueObjectMap(),updatelist);
    }

}
