# table-store
表格存储（tablestore）

## 使用
具体可以查看sdk中的demo目录

1. 定义表对应的entity对象，使用@TableEntity(name = "table_name_test")，table_name_test为表名
2. 属性使用TableKey，TableColumn注解，TableKey是使用在主键上，TableKey中sort必填，必须连续，且要和表格存储创建表的顺序一致；type需要和创建时一致
3. 如果在spring中使用的话，建议每个instance创建一个service，这个instance里面的表操作都应该使用这个service，不同的表initTable不同的表名就行；每次调用，必须先向initTable传入tablename和table对象，得到StoreTableRow，然后可以调用StoreTableRow的方法进行操作


## 示例
instance 的service需要继承TableStoreService

```

	@Service
	public class ActionTableStoreServiceImpl extends TableStoreService {
	
	    @Value("${aliyun.accessId}")
	    private String aliyunAccessId;
	
	    @Value("${aliyun.accessKey}")
	    private String aliyunAccessKey;
	
	    @Value("${aliyun.tablestore.action.endPoint}")
	    private String actionEndPoint;
	
	    @Value("${aliyun.tablestore.action.instanceName}")
	    private String actionInstanceName;
	    
	    
	
	    @PostConstruct
	    public void init() {
	        super.accessId = this.aliyunAccessId;
	        super.accessKey = this.aliyunAccessKey;
	        super.endPoint = this.actionEndPoint;
	        super.instanceName = this.actionInstanceName;
	        initClient();
	    }
	    
	    /***
	    *
	    *
	    *
	    ***/
	}
	
```

定义调用方法：

```

@Service
public class UserActionServiceImpl implements UserActionService {

    private static final Logger logger = LoggerFactory.getLogger(UserActionServiceImpl.class);

    @Autowired
    private ActionTableStoreServiceImpl actionTableStoreService;

    @Autowired
    private IdentityService identityService;

    @Value("${aliyun.tablestore.action.tableName}")
    private String actionTableName;

    @Override
    public void doRecord(UserAction userAction) {
        //设置主键
        long actionId = identityService.genRandomId();
        userAction.setActionId(actionId);
        logger.info("[doRecord] userAction = "+ JSONObject.toJSONString(userAction));
        StoreTableRow row = actionTableStoreService.initTable(actionTableName,userAction);
        if(row == null){
            logger.error("[doRecord] row  init error");
            return;
        }
        actionTableStoreService.putRow(row);
    }
}

```

使用的时候只用实例化UserAction，然后调用doRecord方法即可