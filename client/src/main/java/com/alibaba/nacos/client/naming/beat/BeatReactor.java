/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.client.naming.beat;


import com.alibaba.nacos.client.naming.net.NamingProxy;

import java.util.concurrent.*;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;
import static com.alibaba.nacos.api.common.Constants.DEFAULT_HEART_BEAT_INTERVAL;

/**
 * @author harold
 */
public class BeatReactor {

    private ScheduledExecutorService executorService;

    private BeatBatchTask batchTask;

    public BeatReactor(NamingProxy serverProxy) {
        executorService = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("com.alibaba.nacos.naming.beat.sender");
                return thread;
            }
        });
        batchTask = new BeatBatchTask(executorService, serverProxy);
        executorService.schedule(batchTask, DEFAULT_HEART_BEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public void addBeatInfo(String serviceName, BeatInfo beatInfo) {
        NAMING_LOGGER.info("[BEAT] adding beat: {} to beat map.", beatInfo);
        batchTask.addBeatInfo(serviceName, beatInfo);
    }

    public void removeBeatInfo(String serviceName, String ip, int port) {
        NAMING_LOGGER.info("[BEAT] removing beat: {}:{}:{} from beat map.", serviceName, ip, port);
        batchTask.removeBeatInfo(serviceName, ip, port);
    }

}
