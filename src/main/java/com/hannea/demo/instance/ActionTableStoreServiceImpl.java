package com.hannea.demo.instance;

import com.hannea.impl.TableStoreService;

import javax.annotation.PostConstruct;

/**
 * Class
 *
 * @author wgm
 * @date 2017/11/28
 */
public class ActionTableStoreServiceImpl extends TableStoreService {

    private String aliyunAccessId = "your accessId";

    private String aliyunAccessKey = "your accessKey";

    private String actionEndPoint = "your endPoint";

    private String actionInstanceName = "your instanceName";

    @PostConstruct
    public void init() {
        super.accessId = this.aliyunAccessId;
        super.accessKey = this.aliyunAccessKey;
        super.endPoint = this.actionEndPoint;
        super.instanceName = this.actionInstanceName;
        initClient();
    }
}
