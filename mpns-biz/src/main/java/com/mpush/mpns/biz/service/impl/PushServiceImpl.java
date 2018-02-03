/*
 * (C) Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     ohun@live.cn (夜色)
 */

package com.mpush.mpns.biz.service.impl;

import com.mpush.api.Constants;
import com.mpush.api.push.*;
import com.mpush.api.router.ClientLocation;
import com.mpush.mpns.biz.domain.OfflineMsg;
import com.mpush.mpns.biz.service.PushService;
import com.mpush.tools.Jsons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;

/**
 * Created by ohun on 16/9/15.
 *
 * @author ohun@live.cn (夜色)
 */
@Service
public class PushServiceImpl implements PushService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Resource
    protected PushSender mpusher;
//
//    @Resource
//    private MySqlDaoImpl mySqlDao;

//    private final AtomicLong msgIdSeq = new AtomicLong(1);//TODO业务自己处理

    @Override
    public boolean notify(String channel,String userId, PushMsg pushMsg,PushCallback callback) {
        byte[] content = Jsons.toJson(pushMsg).getBytes(Constants.UTF_8);
        doSend(channel,userId, content,callback);
        return true;
    }

    @Override
    public boolean send(String channel,String userId, byte[] content) {
        doSend(channel,userId, content, new PushCallback() {
            int retryCount = 0;

            @Override
            public void onSuccess(String userId, ClientLocation location) {
                logger.warn("send msg success");
            }

            @Override
            public void onFailure(String userId, ClientLocation clientLocation) {
                saveOfflineMsg(new OfflineMsg(userId, content));
            }

            @Override
            public void onOffline(String userId, ClientLocation clientLocation) {
                saveOfflineMsg(new OfflineMsg(userId, content));
            }

            @Override
            public void onTimeout(String userId, ClientLocation clientLocation) {
                if (retryCount++ > 1) {
                    saveOfflineMsg(new OfflineMsg(userId, content));
                } else {
                    doSend(channel,userId, content, this);
                }
            }
        });
        return true;
    }


    /**
     * 发送规则：先判断userId，userId为空才会判断userIds
     * @param userId
     * @param content
     * @param callback
     * @return
     */
    public FutureTask<PushResult> doSend(String channel, String userId, byte[] content, PushCallback callback) {
        String[] userArrary = userId.split(",");
        List<String> userIdList = new ArrayList<>();
        if (userArrary.length > 1){
            for (String s : userArrary){
                userIdList.add(channel +"_"+s);
            }
            userId = null;
        } else {
            userId = channel +"_"+userId;
        }
        return mpusher.send(new PushContext(content)
                .setUserId(userId)
                .setUserIds(userIdList)
                .setCallback(callback)
        );
    }

    private void saveOfflineMsg(OfflineMsg offlineMsg) {
        logger.info("save offline msg to db");
    }

}
