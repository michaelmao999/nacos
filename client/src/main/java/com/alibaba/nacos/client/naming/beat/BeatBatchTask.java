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

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.net.NamingProxy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.nacos.api.common.Constants.DEFAULT_HEART_BEAT_INTERVAL;

/**
 * @author michael
 */
public class BeatBatchTask implements Runnable{

    private ScheduledExecutorService executorService;

    private NamingProxy serverProxy;

    private List<BeatInfo> beatInfos = new ArrayList<BeatInfo>();

    private final Map<String, Integer> dom2Beat = new HashMap<String, Integer>();

    private Lock lock = new ReentrantLock();

    public BeatBatchTask(ScheduledExecutorService executorService, NamingProxy serverProxy) {
        this.executorService = executorService;
        this.serverProxy = serverProxy;
    }

    public void addBeatInfo(String serviceName, BeatInfo beatInfo) {
        String key = buildKey(serviceName, beatInfo.getIp(), beatInfo.getPort());
        lock.lock();
        try {
            Integer pos = dom2Beat.remove(key);
            if (pos == null) {
                beatInfos.add(beatInfo);
                dom2Beat.put(key, beatInfos.size() - 1);
            }else {
                beatInfos.set(pos, beatInfo);
            }
        }finally {
            lock.unlock();
        }
        MetricsMonitor.getDom2BeatSizeMonitor().set(dom2Beat.size());
    }

    public void removeBeatInfo(String serviceName, String ip, int port) {
        String key = buildKey(serviceName, ip, port);
        lock.lock();
        try {
            Integer pos = dom2Beat.remove(key);
            if (pos == null) {
                return;
            }
            BeatInfo beatInfo = beatInfos.get(pos);
            beatInfo.setStopped(true);
        }finally {
            lock.unlock();
        }
        MetricsMonitor.getDom2BeatSizeMonitor().set(dom2Beat.size());
    }


    private String buildKey(String serviceName, String ip, int port) {
        return serviceName + Constants.NAMING_INSTANCE_ID_SPLITTER
            + ip + Constants.NAMING_INSTANCE_ID_SPLITTER + port;
    }


    @Override
    public void run() {
        if (beatInfos == null) {
            return;
        }
        int len = beatInfos.size();

        List<BeatInfo> batchbeatList = new ArrayList<BeatInfo>();
        for (int index = 0; index < len; index++) {
            BeatInfo beatInfo = beatInfos.get(index);
            if (beatInfo.isStopped()) {
                continue;
            }
            batchbeatList.add(beatInfo);
        }
        long result = serverProxy.sendBatchBeat(batchbeatList);
        long nextTime = result > 0 ? result : DEFAULT_HEART_BEAT_INTERVAL;
        executorService.schedule(this, nextTime, TimeUnit.MILLISECONDS);
    }
}
