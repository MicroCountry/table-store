package com.hannea.demo;

import com.hannea.demo.instance.ActionTableStoreServiceImpl;
import com.hannea.tablestore.StoreTableRow;

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
        userAction.setActionType(1);
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
}
