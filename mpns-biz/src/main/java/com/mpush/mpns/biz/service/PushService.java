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

package com.mpush.mpns.biz.service;

import com.mpush.api.push.PushCallback;
import com.mpush.api.push.PushMsg;
import com.mpush.api.push.PushResult;
import com.mpush.mpns.biz.domain.NotifyDO;

import java.util.concurrent.FutureTask;

/**
 * Created by ohun on 16/9/15.
 *
 * @author ohun@live.cn (夜色)
 */
public interface PushService {

    boolean notify(String channel,String userId, PushMsg pushMsg,PushCallback callback);

    boolean send(String channel,String userId, byte[] content);

    /**
     * 发送规则：先判断userId，userId为空才会判断userIds
     * @param userId
     * @param content
     * @param callback
     * @return
     */
    FutureTask<PushResult> doSend(String channel,String userId, byte[] content, PushCallback callback);
}
