package com.hannea.impl;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.*;
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
}
