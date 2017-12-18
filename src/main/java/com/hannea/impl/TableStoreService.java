package com.hannea.impl;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.model.internal.CreateTableRequestEx;
import com.hannea.annotation.TableColumn;
import com.hannea.annotation.TableEntity;
import com.hannea.annotation.TableKey;
import com.hannea.constant.ColumnTypeObject;
import com.hannea.tablestore.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.*;


/**
 * Class
 * 调用之前必须调用initTable 进行设置，否则会出现问题
 * @author wgm
 * @date 2017/11/03
 */
public abstract class TableStoreService implements ITableStoreService {

    private static final Logger logger = LoggerFactory.getLogger(TableStoreService.class);

    protected String accessId;

    protected String accessKey;

    protected String endPoint;

    protected String instanceName;

    protected SyncClient syncClient;

    @Override
    public boolean initClient() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        // 设置建立连接的超时时间
        clientConfiguration.setConnectionTimeoutInMillisecond(5000);
        // 设置socket超时时间
        clientConfiguration.setSocketTimeoutInMillisecond(5000);
        // 设置重试策略，若不设置，采用默认的重试策略
        //clientConfiguration.setRetryStrategy(new AlwaysRetryStrategy());
        //建立连接
        syncClient = new SyncClient(endPoint, accessId,
                accessKey, instanceName, clientConfiguration);

        return true;
    }

    private List<Column> covert(Map<String, ColumnValueObject> column) {
        List<Column> list = new ArrayList<Column>();
        for (String ck : column.keySet()) {
            ColumnValueObject cv = column.get(ck);
            Column c = null;
            switch (cv.getType()) {
                case INTEGER:
                    c = new Column(ck, ColumnValue.fromLong(Long.valueOf(String.valueOf(cv.getValue()))));
                    break;
                case STRING:
                    c = new Column(ck, ColumnValue.fromString(String.valueOf(cv.getValue())));
                    break;
                case BOOLEAN:
                    c = new Column(ck, ColumnValue.fromBoolean((Boolean) cv.getValue()));
                    break;
                case DOUBLE:
                    c = new Column(ck, ColumnValue.fromDouble(Double.valueOf(String.valueOf(cv.getValue()))));
                    break;
                case BINARY:
                    c = new Column(ck, ColumnValue.fromBinary((byte[]) cv.getValue()));
                    break;
                default:
                    break;
            }
            if (c != null) {
                list.add(c);
            }
        }
        return list;
    }

    private List<Column> covertSelective(Map<String, ColumnValueObject> column) {
        List<Column> list = new ArrayList<Column>();
        for (String ck : column.keySet()) {
            ColumnValueObject cv = column.get(ck);
            Column c = null;
            if(cv.getValue() == null){
                continue;
            }
            switch (cv.getType()) {
                case INTEGER:
                    c = new Column(ck, ColumnValue.fromLong(Long.valueOf(String.valueOf(cv.getValue()))));
                    break;
                case STRING:
                    c = new Column(ck, ColumnValue.fromString(String.valueOf(cv.getValue())));
                    break;
                case BOOLEAN:
                    c = new Column(ck, ColumnValue.fromBoolean((Boolean) cv.getValue()));
                    break;
                case DOUBLE:
                    c = new Column(ck, ColumnValue.fromDouble(Double.valueOf(String.valueOf(cv.getValue()))));
                    break;
                case BINARY:
                    c = new Column(ck, ColumnValue.fromBinary((byte[]) cv.getValue()));
                    break;
                default:
                    break;
            }
            if (c != null) {
                list.add(c);
            }
        }
        return list;
    }

    private PrimaryKey buildKey(Map<String, PrimaryKeyValueObject> primaryKey) {
        // 主键
        int size = primaryKey.size();
        PrimaryKeyColumn[] arr = new PrimaryKeyColumn[size];

        for (String pk : primaryKey.keySet()) {
            PrimaryKeyValueObject pkv = primaryKey.get(pk);
            switch (pkv.getType()) {
                case INTEGER:
                    PrimaryKeyValue valueInt= PrimaryKeyValue.fromLong(Long.valueOf(String.valueOf(pkv.getValue())));
                    Preconditions.checkArgument(pk != null && !pk.isEmpty(), "The name of primary key should not be null or empty.");
                    Preconditions.checkNotNull(valueInt, "The value of primary key should not be null.");
                    arr[pkv.getSort()] = new PrimaryKeyColumn(pk, valueInt);
                    break;
                case STRING:
                    PrimaryKeyValue valueStr= PrimaryKeyValue.fromString(String.valueOf(pkv.getValue()));
                    Preconditions.checkArgument(pk != null && !pk.isEmpty(), "The name of primary key should not be null or empty.");
                    Preconditions.checkNotNull(valueStr, "The value of primary key should not be null.");
                    arr[pkv.getSort()] = new PrimaryKeyColumn(pk, valueStr);
                    break;
                case BINARY:
                    PrimaryKeyValue valueBinary= PrimaryKeyValue.fromString(String.valueOf(pkv.getValue()));
                    Preconditions.checkArgument(pk != null && !pk.isEmpty(), "The name of primary key should not be null or empty.");
                    Preconditions.checkNotNull(valueBinary, "The value of primary key should not be null.");
                    arr[pkv.getSort()] = new PrimaryKeyColumn(pk, valueBinary);
                    break;
                default:
                    break;
            }
        }
        PrimaryKey primaryKeys = new PrimaryKey(arr);
        return primaryKeys;
    }


    @Override
    public boolean exist(StoreTable table) {
        return exist(table.getTableName());
    }

    public boolean exist(String tableName) {
        DescribeTableRequest request = new DescribeTableRequest(tableName);
        try {
            DescribeTableResponse response = syncClient.describeTable(request);
            TableMeta tableMeta = response.getTableMeta();
            if (tableMeta != null && tableMeta.getTableName() != null) {
                return true;
            }
        } catch (TableStoreException e) {
            return false;
        } catch (ClientException e) {
            return false;
        }
        return false;
    }

    @Override
    public boolean createTable(StoreTable table) {
        TableMeta tableMeta = new TableMeta(table.getTableName());
        List<PrimaryKeySchemaObject> primaryKey = table.getPrimaryKeySchemaObjectList();
        for (PrimaryKeySchemaObject k : primaryKey) {

            if (k.getOption() != null) {
                switch (k.getType()) {
                    case INTEGER:
                        tableMeta.addPrimaryKeyColumn(
                                new PrimaryKeySchema(k.getName(), PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
                        break;
                    case STRING:
                        tableMeta.addPrimaryKeyColumn(
                                new PrimaryKeySchema(k.getName(), PrimaryKeyType.STRING, PrimaryKeyOption.AUTO_INCREMENT));
                        break;
                    case BINARY:
                        tableMeta.addPrimaryKeyColumn(
                                new PrimaryKeySchema(k.getName(), PrimaryKeyType.BINARY, PrimaryKeyOption.AUTO_INCREMENT));
                        break;
                    default:
                        break;
                }
            } else {
                switch (k.getType()) {
                    case INTEGER:
                        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(k.getName(), PrimaryKeyType.INTEGER));
                        break;
                    case STRING:
                        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(k.getName(), PrimaryKeyType.STRING));
                        break;
                    case BINARY:
                        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(k.getName(), PrimaryKeyType.BINARY));
                        break;
                    default:
                        break;
                }
            }
        }
        // 数据的过期时间 单位 -1代表永不过期. 为 365 * 24 * 3600.
        int timeToLive = -1;
        // 保存的最大版本数
        int maxVersions = 1;
        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);
        CreateTableRequestEx request = new CreateTableRequestEx(tableMeta, tableOptions);
        CreateTableResponse r = syncClient.createTable(request);
        return r.getRequestId() != null;
    }

    @Override
    public boolean putRow(StoreTableRow row) {
        return putRow(row.getTableName(), row.getPrimaryKeyValueObjectMap(), row.getColumnValueObjectMap());
    }

    public boolean putRow(String tableName, Map<String, PrimaryKeyValueObject> primaryKey,
                          Map<String, ColumnValueObject> column) {
        if(StringUtils.isBlank(tableName)){
                logger.error("[putRow] you need set table name first,put error");
                return false;
        }
        PrimaryKey primaryKeys = buildKey(primaryKey);
        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKeys);
        List<Column> list = covert(column);
        for (Column c : list) {
            rowPutChange.addColumn(c);
        }
        PutRowResponse r = syncClient.putRow(new PutRowRequest(rowPutChange));
        return r.getRequestId() != null;
    }

    @Override
    public boolean putRowSelective(StoreTableRow row) {
        return putRowSelective(row.getTableName(), row.getPrimaryKeyValueObjectMap(), row.getColumnValueObjectMap());
    }

    public boolean putRowSelective(String tableName, Map<String, PrimaryKeyValueObject> primaryKey,
                          Map<String, ColumnValueObject> column) {
        if(StringUtils.isBlank(tableName)){
                logger.error("[putRowSelective] you need set table name first,put error");
                return false;
        }
        PrimaryKey primaryKeys = buildKey(primaryKey);
        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKeys);
        List<Column> list = covertSelective(column);
        for (Column c : list) {
            rowPutChange.addColumn(c);
        }
        PutRowResponse r = syncClient.putRow(new PutRowRequest(rowPutChange));
        return r.getRequestId() != null;
    }

    @Override
    public boolean getRow(StoreTableRow row) {
        PrimaryKey primaryKeys = buildKey(row.getPrimaryKeyValueObjectMap());
        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(row.getTableName(), primaryKeys);
        // 设置读取版本
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = syncClient.getRow(new GetRowRequest(criteria));
        if (getRowResponse == null) {
            return false;
        }
        Row rows = getRowResponse.getRow();
        if (rows == null) {
            return false;
        }
        Column[] cols = rows.getColumns();
        Map<String, ColumnValueObject> v = new LinkedHashMap<>();
        for (Column c : cols) {
            ColumnValue cv = c.getValue();
            ColumnValueObject cvo = null;
            switch (cv.getType()) {
                case STRING:
                    cvo = new ColumnValueObject(cv.asString(), ColumnTypeObject.STRING);
                    break;
                case INTEGER:
                    cvo = new ColumnValueObject(cv.asLong(), ColumnTypeObject.INTEGER);
                    break;
                case BOOLEAN:
                    cvo = new ColumnValueObject(cv.asBoolean(), ColumnTypeObject.BOOLEAN);
                    break;
                case DOUBLE:
                    cvo = new ColumnValueObject(cv.asDouble(), ColumnTypeObject.DOUBLE);
                    break;
                case BINARY:
                    cvo = new ColumnValueObject(cv.asBinary(), ColumnTypeObject.BINARY);
                    break;

                default:
                    break;
            }
            v.put(c.getName(), cvo);
        }
        row.setColumnValueObjectMap(v);
        return getRowResponse.getRequestId() != null;
    }

    @Override
    public boolean deleteRow(StoreTableRow row) {
        PrimaryKey primaryKeys = buildKey(row.getPrimaryKeyValueObjectMap());
        RowDeleteChange rowDeleteChange = new RowDeleteChange(row.getTableName(), primaryKeys);
        DeleteRowResponse r = syncClient.deleteRow(new DeleteRowRequest(rowDeleteChange));
        return r.getRequestId() != null;
    }

    @Override
    public boolean updateRowSelective(StoreTableRow row) {
        PrimaryKey primaryKeys = buildKey(row.getPrimaryKeyValueObjectMap());
        RowUpdateChange rowUpdateChange = new RowUpdateChange(row.getTableName(), primaryKeys);
        List<Column> list = covertSelective(row.getColumnValueObjectMap());
        for (Column c : list) {
            rowUpdateChange.put(c);
        }
        UpdateRowResponse r = syncClient.updateRow(new UpdateRowRequest(rowUpdateChange));
        return r.getRequestId() != null;
    }

    @Override
    public boolean updateRow(StoreTableRow row) {
        PrimaryKey primaryKeys = buildKey(row.getPrimaryKeyValueObjectMap());
        RowUpdateChange rowUpdateChange = new RowUpdateChange(row.getTableName(), primaryKeys);
        List<Column> list = covert(row.getColumnValueObjectMap());
        for (Column c : list) {
            rowUpdateChange.put(c);
        }
        UpdateRowResponse r = syncClient.updateRow(new UpdateRowRequest(rowUpdateChange));
        return r.getRequestId() != null;
    }

    @Override
    public boolean updateRow(String tablName, Map<String, PrimaryKeyValueObject> primaryKeyMap,List<Column> list){
        PrimaryKey primaryKeys = buildKey(primaryKeyMap);
        RowUpdateChange rowUpdateChange = new RowUpdateChange(tablName, primaryKeys);
        for (Column c : list) {
            rowUpdateChange.put(c);
        }
        UpdateRowResponse r = syncClient.updateRow(new UpdateRowRequest(rowUpdateChange));
        return r.getRequestId() != null;
    }

    /**
     * 请注意column支持int long double bigdecimal
     * @param storeTableRow
     * @param columnNameList
     * @return
     */
    @Override
    public boolean updateIncrement(StoreTableRow storeTableRow, List<String> columnNameList){
        PrimaryKey primaryKeys = buildKey(storeTableRow.getPrimaryKeyValueObjectMap());
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(storeTableRow.getTableName());
        criteria.setPrimaryKey(primaryKeys);
        criteria.setMaxVersions(1);
        GetRowRequest request = new GetRowRequest(criteria);
        RowUpdateChange rowUpdateChange = new RowUpdateChange(storeTableRow.getTableName(), primaryKeys);
        if (columnNameList != null) {
            Map<String, ColumnValueObject> map = storeTableRow.getColumnValueObjectMap();
            Map<String, PrimaryKeyValueObject> primaryMap = storeTableRow.getPrimaryKeyValueObjectMap();
            Map<String, ColumnValueObject> colMap = new HashMap<>();
            for(Map.Entry<String,ColumnValueObject> entry : map.entrySet()){
                if(!(columnNameList.contains(entry.getKey()) || primaryMap.containsKey(entry.getKey()))){
                    colMap.put(entry.getKey(),entry.getValue());
                }
            }
            for (String colName : columnNameList) {
                while (true) {
                    boolean succeed = getAndSet(request, rowUpdateChange, colName, map,colMap);
                    if (!succeed) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                        }
                    } else {
                        break;
                    }
                }
            }

        }
        return true;
    }

    @Override
    public boolean updateIncrementAfterRowCheck(StoreTableRow storeTableRow, Row row, List<String> columnNameList){
        PrimaryKey primaryKeys = buildKey(storeTableRow.getPrimaryKeyValueObjectMap());
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(storeTableRow.getTableName());
        criteria.setPrimaryKey(primaryKeys);
        criteria.setMaxVersions(1);
        GetRowRequest request = new GetRowRequest(criteria);
        RowUpdateChange rowUpdateChange = new RowUpdateChange(storeTableRow.getTableName(), primaryKeys);
        if(columnNameList != null) {
            Map<String, ColumnValueObject> map = storeTableRow.getColumnValueObjectMap();
            Map<String, PrimaryKeyValueObject> primaryMap = storeTableRow.getPrimaryKeyValueObjectMap();
            Map<String, ColumnValueObject> colMap = new HashMap<>();
            for(Map.Entry<String,ColumnValueObject> entry : map.entrySet()){
                if(!(columnNameList.contains(entry.getKey()) || primaryMap.containsKey(entry.getKey()))){
                    colMap.put(entry.getKey(),entry.getValue());
                }
            }
            for(String colName : columnNameList) {
                while (true) {
                    boolean succeed = getAndSet(request, rowUpdateChange, colName, storeTableRow.getColumnValueObjectMap(),colMap);
                    if (!succeed) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        return true;
    }

    public boolean getAndSet(GetRowRequest request, RowUpdateChange rowUpdateChange,String columnName,Map<String,ColumnValueObject> map,Map<String,ColumnValueObject> colMap){
        Row row = this.syncClient.getRow(request).getRow();
        if(columnName != null ) {
            ColumnValueObject columnValueObject = map.get(columnName);
            if(columnValueObject != null && columnValueObject.getType() == ColumnTypeObject.STRING){
                rowUpdateChange.put(columnName, ColumnValue.fromString(columnValueObject.getValue().toString()));
            }else if(columnValueObject != null && columnValueObject.getType() != ColumnTypeObject.STRING) {
                Condition cond = new Condition();
                if (row != null) {
                    Column column = row.getLatestColumn(columnName);
                    if (column != null) {
                        ColumnValue oldColumnValue = column.getValue();
                        SingleColumnValueCondition single = new SingleColumnValueCondition(columnName,
                                SingleColumnValueCondition.CompareOperator.EQUAL, oldColumnValue);
                        cond.setColumnCondition(single);
                        rowUpdateChange.put(columnName, newColumnValue(columnName, oldColumnValue, map));
                    } else {
                        SingleColumnValueCondition single = new SingleColumnValueCondition(columnName,
                                SingleColumnValueCondition.CompareOperator.EQUAL, oldNoneColumValue(columnName, map));
                        cond.setColumnCondition(single);
                        rowUpdateChange.put(columnName, newColumnValue(columnName, null, map));
                    }
                    rowUpdateChange.setCondition(cond);
                } else {
                    SingleColumnValueCondition single = new SingleColumnValueCondition(columnName,
                            SingleColumnValueCondition.CompareOperator.EQUAL, oldNoneColumValue(columnName, map));
                    cond.setColumnCondition(single);
                    rowUpdateChange.put(columnName, newColumnValue(columnName, null, map));
                    for (Map.Entry<String, ColumnValueObject> entry : colMap.entrySet()) {
                        rowUpdateChange.put(entry.getKey(), newColumnValue(entry.getKey(), null, map));
                    }
                }
            }
        }

        try {
            syncClient.updateRow(new UpdateRowRequest(rowUpdateChange));
        } catch (TableStoreException e) {
            if (e.getErrorCode().equals("OTSConditionCheckFail")) {
                return false;
            }
        }
        return true;
    }

    private ColumnValue oldNoneColumValue(String columnName,Map<String,ColumnValueObject> map){
        ColumnValueObject valueObject = map.get(columnName);
        if(valueObject.getType() == ColumnTypeObject.INTEGER ){
            return ColumnValue.fromLong(0);
        }else if(valueObject.getType() == ColumnTypeObject.DOUBLE){
            return ColumnValue.fromDouble(BigDecimal.ZERO.doubleValue());
        }else if(valueObject.getType() == ColumnTypeObject.STRING){
            return ColumnValue.fromString("");
        }
        return null;
    }
    private ColumnValue newColumnValue(String columnName,ColumnValue oldColumnValue,Map<String,ColumnValueObject> map){
        ColumnValueObject valueObject = map.get(columnName);
        if(valueObject.getType() == ColumnTypeObject.INTEGER ){
            return oldColumnValue == null ? ColumnValue.fromLong(Long.valueOf(String.valueOf(valueObject.getValue()))) :ColumnValue.fromLong(oldColumnValue.asLong() + Long.valueOf(String.valueOf(valueObject.getValue())));
        }else if(valueObject.getType() == ColumnTypeObject.DOUBLE){
            return oldColumnValue == null ? ColumnValue.fromDouble(new BigDecimal(String.valueOf(valueObject.getValue())).setScale(2,BigDecimal.ROUND_DOWN).doubleValue()) : ColumnValue.fromDouble(new BigDecimal(String.valueOf(oldColumnValue.asDouble())).add(new BigDecimal(String.valueOf(valueObject.getValue()))).setScale(2,BigDecimal.ROUND_DOWN).doubleValue());
        }else if(valueObject.getType() == ColumnTypeObject.STRING){
            return ColumnValue.fromString(String.valueOf(valueObject.getValue()));
        }
        return null;
    }

    @Override
    public StoreTableRow initTable(String tableName,Object object) {
        /**
         * 主键
         */
         Map<String, PrimaryKeyValueObject> primaryKeyValue = new HashMap<>();
        /**
         * 属性列
         */
        Map<String, ColumnValueObject> columnValueObjectMap = new HashMap<>();

        String tableEntityName = null;
        try {
            Field[] fields = object.getClass().getDeclaredFields();
            TableEntity tableEntity = object.getClass().getAnnotation(TableEntity.class);
            if(tableEntity instanceof TableEntity){
                tableEntityName = tableEntity.name();
                logger.info("[initTable] tableEntityName :"+tableEntityName);
            }else {
                logger.info("[initTable] not set tableEntityName ");
            }
            if(!StringUtils.isBlank(tableName)){
                tableEntityName = tableName;
            }
            for (Field field : fields) {
                field.setAccessible(true);
                TableKey annotationTableKey = field.getAnnotation(TableKey.class);
                if(annotationTableKey != null) {
                    //主键 主键不可以为空
                    String keyName = StringUtils.isBlank(annotationTableKey.name()) ? field.getName() : annotationTableKey.name();
                    if(field.get(object) == null){
                        logger.error("[initTable] primary key cannot be null,keyName = "+keyName);
                        return null;
                    }
                    primaryKeyValue.put(keyName,new PrimaryKeyValueObject(field.get(object), annotationTableKey.type(),annotationTableKey.sort()));
                }
                TableColumn annotationTableColumn = field.getAnnotation(TableColumn.class);
                if(annotationTableColumn != null) {
                    //属性列
                    String keyName = StringUtils.isBlank(annotationTableColumn.name()) ? field.getName() : annotationTableColumn.name();
                    columnValueObjectMap.put(keyName,new ColumnValueObject(field.get(object), annotationTableColumn.type()));
                }
            }
        }catch (IllegalAccessException e) {
            logger.error("[initTable] error :"+ e);
            return null;
        }
        if(primaryKeyValue.isEmpty()){
            logger.error("[initTable] primary key none");
            return null;
        }
        StoreTableRow row = new StoreTableRow();
        row.setTableName(tableEntityName);
        row.setColumnValueObjectMap(columnValueObjectMap);
        row.setPrimaryKeyValueObjectMap(primaryKeyValue);
        return row;
    }

    @Override
    public List<Row> batchGetRow(StoreTableRow row) {
        BatchGetRowRequest request = new BatchGetRowRequest();
        MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(row.getTableName());
        PrimaryKey primaryKeys = buildKey(row.getPrimaryKeyValueObjectMap());
        multiRowQueryCriteria.addRow(primaryKeys);
        Set<String> set = row.getColumnValueObjectMap().keySet();
        multiRowQueryCriteria.addColumnsToGet(set);
        request.addMultiRowQueryCriteria(multiRowQueryCriteria);
        BatchGetRowResponse response = syncClient.batchGetRow(request);
        List<Row> rows = new ArrayList<>();
        if(response.isAllSucceed()) {
            List<BatchGetRowResponse.RowResult> list = response.getBatchGetRowResult(row.getTableName());
            for (BatchGetRowResponse.RowResult rowResult : list) {
                rows.add(rowResult.getRow());
            }
        }
        return rows;
    }

    @Override
    public List<Row> getRange(StoreTableRow storeTableRow, Filter filter, Map<String, PrimaryKeyValue> startPkValue, Map<String,PrimaryKeyValue> endPkValue) {
        RangeIteratorParameter rangeIteratorParameter = new RangeIteratorParameter(storeTableRow.getTableName());

        // 设置起始主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for(Map.Entry<String,PrimaryKeyValue> entry : startPkValue.entrySet()) {
            primaryKeyBuilder.addPrimaryKeyColumn(entry.getKey(), entry.getValue());
        }
        rangeIteratorParameter.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());

        // 设置结束主键
        primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for(Map.Entry<String,PrimaryKeyValue> entry : endPkValue.entrySet()) {
            primaryKeyBuilder.addPrimaryKeyColumn(entry.getKey(), entry.getValue());
        }
        rangeIteratorParameter.setExclusiveEndPrimaryKey(primaryKeyBuilder.build());

        rangeIteratorParameter.setMaxVersions(1);
        if(filter != null){
            rangeIteratorParameter.setFilter(filter);
        }
        Iterator<Row> iterator = syncClient.createRangeIterator(rangeIteratorParameter);
        List<Row> rows = new ArrayList<>();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            rows.add(row);
        }
        return rows;
    }

    /**
     * 只支持对 int long 以及 double的操作
     * @param tableName
     * @param primaryKey
     * @param columnName
     * @param increment
     */
    @Override
    public void atomicIncrement(String tableName, PrimaryKey primaryKey, String columnName, Object increment) {
        while (true) {
            boolean succeed = getAndSet(tableName, primaryKey, columnName, increment);
            if (!succeed) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
            } else {
                break;
            }
        }
    }

    @Override
    public boolean getAndSet(String tableName, PrimaryKey primaryKey, String columnName, Object increment) {
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);
        criteria.setMaxVersions(1);

        GetRowRequest request = new GetRowRequest(criteria);
        Row row = this.syncClient.getRow(request).getRow();
        //检查递增的类型
        if(!(increment instanceof Integer || increment instanceof Double || increment instanceof Long)){
            throw new IllegalStateException("The type of column you want to do increment is not INTEGER or Double or Long.");
        }
        long initLongValue = 0;
        double initDoubleValue = 0.0;
        ColumnValue oldValue = null;
        if (row != null) {
            Column column = row.getLatestColumn(columnName);
            if (column != null && (increment instanceof Integer || increment instanceof Long) && column.getValue().getType() != ColumnType.INTEGER ) {
                throw new IllegalStateException("The type of column you want to do increment is not INTEGER.");
            }
            if(column != null && (increment instanceof Double) && column.getValue().getType() != ColumnType.DOUBLE){
                throw new IllegalStateException("The type of column you want to do increment is not Double.");
            }
            if (column != null && (increment instanceof Integer || increment instanceof Long) ) {
                initLongValue = column.getValue().asLong();
                oldValue = ColumnValue.fromLong(initLongValue);
            }
            if (column != null && (increment instanceof Double) ) {
                initDoubleValue = column.getValue().asDouble();
                oldValue = ColumnValue.fromDouble(initDoubleValue);
            }
        }
        ColumnValue newValue;
        if(increment instanceof Integer || increment instanceof Long) {
            newValue = ColumnValue.fromLong((initLongValue + ((long)increment)));
        }else {
            newValue = ColumnValue.fromDouble(new BigDecimal(String.valueOf(initDoubleValue)).add(new BigDecimal(String.valueOf(increment))).setScale(2,BigDecimal.ROUND_DOWN).doubleValue());
        }
        RowUpdateChange rowChange = new RowUpdateChange(tableName, primaryKey);
        rowChange.put(columnName, newValue);

        Condition cond = new Condition();
        SingleColumnValueCondition columnCondition = new SingleColumnValueCondition(columnName,
                SingleColumnValueCondition.CompareOperator.EQUAL, oldValue);
        columnCondition.setPassIfMissing(true);
        columnCondition.setLatestVersionsOnly(true);
        cond.setColumnCondition(columnCondition);
        rowChange.setCondition(cond);

        try {
            UpdateRowRequest updateRowRequest = new UpdateRowRequest(rowChange);
            this.syncClient.updateRow(updateRowRequest);
        } catch (TableStoreException e) {
            if (e.getErrorCode().equals("OTSConditionCheckFail")) {
                return false;
            }
        }
        return true;
    }

    public void setFieldValue(Map<String, Object> map, Object bean) throws Exception{
        Class<?> cls = bean.getClass();
        Method methods[] = cls.getDeclaredMethods();
        Field fields[] = cls.getDeclaredFields();

        for(Field field:fields){
            String fldtype = field.getType().getSimpleName();
            String fldSetName = field.getName();
            String setMethod = pareSetName(fldSetName);
            if(!checkMethod(methods, setMethod)){
                continue;
            }
            Object value = map.get(fldSetName);
            Method method = cls.getMethod(setMethod, field.getType());
            if(null != value){
                if("String".equals(fldtype)){
                    method.invoke(bean, String.valueOf(value));
                }else if("Double".equals(fldtype)){
                    method.invoke(bean, (Double)value);
                }else if("Integer".equals(fldtype)){
                    int val = Integer.valueOf(String.valueOf(value));
                    method.invoke(bean, val);
                }else if("Long".equalsIgnoreCase(fldtype)){
                    long val = Long.valueOf(String.valueOf(value));
                    method.invoke(bean, val);
                }else if("BigDecimal".equalsIgnoreCase(fldtype)){
                    BigDecimal val = new BigDecimal(String.valueOf(value));
                    method.invoke(bean, val);
                }
            }

        }
    }

    /**
     * 拼接某属性set 方法
     * @param fldname
     * @return
     */
    public String pareSetName(String fldname){
        if(null == fldname || "".equals(fldname)){
            return null;
        }
        String pro = "set"+fldname.substring(0,1).toUpperCase()+fldname.substring(1);
        return pro;
    }
    /**
     * 判断该方法是否存在
     * @param methods
     * @param met
     * @return
     */
    public boolean checkMethod(Method methods[],String met){
        if(null != methods ){
            for(Method method:methods){
                if(met.equals(method.getName())){
                    return true;
                }
            }
        }
        return false;
    }
}
