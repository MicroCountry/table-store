package com.hannea.demo;

import com.hannea.annotation.TableColumn;
import com.hannea.annotation.TableEntity;
import com.hannea.annotation.TableKey;
import com.hannea.constant.ColumnTypeObject;
import com.hannea.constant.PrimaryKeyTypeObject;

/**
 * Class
 * tableKey 中的sort必填，并且值连续，且和表格存储控制台创建的顺序一致
 * @author wgm
 * @date 2017/11/28
 */
@TableEntity(name = "table_name_test")
public class UserAction {


    @TableKey(sort = 0)
    private String hashUserId;

    @TableKey(type = PrimaryKeyTypeObject.INTEGER,sort = 1)
    private long userId;

    @TableKey(type = PrimaryKeyTypeObject.INTEGER,sort = 2)
    private int userType;

    @TableColumn(type = ColumnTypeObject.INTEGER)
    private int actionType;

    @TableColumn(type = ColumnTypeObject.INTEGER)
    private long times;

    public String getHashUserId() {
        return hashUserId;
    }

    public void setHashUserId(String hashUserId) {
        this.hashUserId = hashUserId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public int getUserType() {
        return userType;
    }

    public void setUserType(int userType) {
        this.userType = userType;
    }

    public int getActionType() {
        return actionType;
    }

    public void setActionType(int actionType) {
        this.actionType = actionType;
    }

    public long getTimes() {
        return times;
    }

    public void setTimes(long times) {
        this.times = times;
    }
}
