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
package com.alibaba.nacos.client.naming.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.backups.FailoverReactor;
import com.alibaba.nacos.client.naming.cache.DiskCache;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.client.utils.StringUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * @author xuanyin
 */
public class HostReactor {

    private static final long DEFAULT_DELAY = 1000L;

    private static final long UPDATE_HOLD_INTERVAL = 5000L;

    private Map<String, ServiceInfo> serviceInfoMap;

    private Map<String, Object> updatingMap;

    private PushReceiver pushReceiver;

    private EventDispatcher eventDispatcher;

    private NamingProxy serverProxy;

    private FailoverReactor failoverReactor;

    private String cacheDir;

    private ScheduledExecutorService executor;

    private BatchUpdateTask batchUpdateTask;

    public HostReactor(EventDispatcher eventDispatcher, NamingProxy serverProxy, String cacheDir) {
        this(eventDispatcher, serverProxy, cacheDir, false, UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }

    public HostReactor(EventDispatcher eventDispatcher, NamingProxy serverProxy, String cacheDir,
                       boolean loadCacheAtStart, int pollingThreadCount) {

        executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("com.alibaba.nacos.client.naming.updater");
                return thread;
            }
        });

        this.eventDispatcher = eventDispatcher;
        this.serverProxy = serverProxy;
        this.cacheDir = cacheDir;
        if (loadCacheAtStart) {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(DiskCache.read(this.cacheDir));
        } else {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(16);
        }

        this.updatingMap = new ConcurrentHashMap<String, Object>();
        this.failoverReactor = new FailoverReactor(this, cacheDir);
        this.pushReceiver = new PushReceiver(this);
        batchUpdateTask = new BatchUpdateTask();
        executor.schedule(batchUpdateTask, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }

    public Map<String, ServiceInfo> getServiceInfoMap() {
        return serviceInfoMap;
    }

    public ServiceInfo processServiceJSON(String json) {
        ServiceInfo serviceInfo = JSON.parseObject(json, ServiceInfo.class);
        ServiceInfo oldService = serviceInfoMap.get(serviceInfo.getKey());
        if (serviceInfo.getHosts() == null || !serviceInfo.validate()) {
            //empty or error push, just ignore
            return oldService;
        }

        boolean changed = false;

        if (oldService != null) {

            if (oldService.getLastRefTime() > serviceInfo.getLastRefTime()) {
                NAMING_LOGGER.warn("out of date data received, old-t: " + oldService.getLastRefTime()
                    + ", new-t: " + serviceInfo.getLastRefTime());
            }

            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);

            Map<String, Instance> oldHostMap = new HashMap<String, Instance>(oldService.getHosts().size());
            for (Instance host : oldService.getHosts()) {
                oldHostMap.put(host.toInetAddr(), host);
            }

            Map<String, Instance> newHostMap = new HashMap<String, Instance>(serviceInfo.getHosts().size());
            for (Instance host : serviceInfo.getHosts()) {
                newHostMap.put(host.toInetAddr(), host);
            }

            Set<Instance> modHosts = new HashSet<Instance>();
            Set<Instance> newHosts = new HashSet<Instance>();
            Set<Instance> remvHosts = new HashSet<Instance>();

            List<Map.Entry<String, Instance>> newServiceHosts = new ArrayList<Map.Entry<String, Instance>>(
                newHostMap.entrySet());
            for (Map.Entry<String, Instance> entry : newServiceHosts) {
                Instance host = entry.getValue();
                String key = entry.getKey();
                if (oldHostMap.containsKey(key) && !StringUtils.equals(host.toString(),
                    oldHostMap.get(key).toString())) {
                    modHosts.add(host);
                    continue;
                }

                if (!oldHostMap.containsKey(key)) {
                    newHosts.add(host);
                }
            }

            for (Map.Entry<String, Instance> entry : oldHostMap.entrySet()) {
                Instance host = entry.getValue();
                String key = entry.getKey();
                if (newHostMap.containsKey(key)) {
                    continue;
                }

                if (!newHostMap.containsKey(key)) {
                    remvHosts.add(host);
                }

            }

            if (newHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("new ips(" + newHosts.size() + ") service: "
                    + serviceInfo.getKey() + " -> " + JSON.toJSONString(newHosts));
            }

            if (remvHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("removed ips(" + remvHosts.size() + ") service: "
                    + serviceInfo.getKey() + " -> " + JSON.toJSONString(remvHosts));
            }

            if (modHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("modified ips(" + modHosts.size() + ") service: "
                    + serviceInfo.getKey() + " -> " + JSON.toJSONString(modHosts));
            }

            serviceInfo.setJsonFromServer(json);

            if (newHosts.size() > 0 || remvHosts.size() > 0 || modHosts.size() > 0) {
                eventDispatcher.serviceChanged(serviceInfo);
                DiskCache.write(serviceInfo, cacheDir);
            }

        } else {
            changed = true;
            NAMING_LOGGER.info("init new ips(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getKey() + " -> " + JSON
                .toJSONString(serviceInfo.getHosts()));
            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);
            eventDispatcher.serviceChanged(serviceInfo);
            serviceInfo.setJsonFromServer(json);
            DiskCache.write(serviceInfo, cacheDir);
        }

        MetricsMonitor.getServiceInfoMapSizeMonitor().set(serviceInfoMap.size());

        if (changed) {
            NAMING_LOGGER.info("current ips:(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getKey() +
                " -> " + JSON.toJSONString(serviceInfo.getHosts()));
        }

        return serviceInfo;
    }

    private ServiceInfo getServiceInfo0(String serviceName, String clusters) {

        String key = ServiceInfo.getKey(serviceName, clusters);

        return serviceInfoMap.get(key);
    }

    public ServiceInfo getServiceInfoDirectlyFromServer(final String serviceName, final String clusters) throws NacosException {
        String result = serverProxy.queryList(serviceName, clusters, 0, false);
        if (StringUtils.isNotEmpty(result)) {
            return JSON.parseObject(result, ServiceInfo.class);
        }
        return null;
    }

    public ServiceInfo getServiceInfo(final String serviceName, final String clusters) {

        NAMING_LOGGER.debug("failover-mode: " + failoverReactor.isFailoverSwitch());
        String key = ServiceInfo.getKey(serviceName, clusters);
        if (failoverReactor.isFailoverSwitch()) {
            return failoverReactor.getService(key);
        }

        ServiceInfo serviceObj = getServiceInfo0(serviceName, clusters);

        if (null == serviceObj) {
            serviceObj = new ServiceInfo(serviceName, clusters);

            serviceInfoMap.put(serviceObj.getKey(), serviceObj);

            updatingMap.put(serviceName, new Object());
            updateServiceNow(serviceName, clusters);
            updatingMap.remove(serviceName);

        } else if (updatingMap.containsKey(serviceName)) {

            if (UPDATE_HOLD_INTERVAL > 0) {
                // hold a moment waiting for update finish
                synchronized (serviceObj) {
                    try {
                        serviceObj.wait(UPDATE_HOLD_INTERVAL);
                    } catch (InterruptedException e) {
                        NAMING_LOGGER.error("[getServiceInfo] serviceName:" + serviceName + ", clusters:" + clusters, e);
                    }
                }
            }
        }

        scheduleUpdateIfAbsent(serviceName, clusters);

        return serviceInfoMap.get(serviceObj.getKey());
    }

    private void scheduleUpdateIfAbsent(String serviceName, String clusters) {
        batchUpdateTask.addService(serviceName, clusters);
    }

    private void updateServiceNow(String serviceName, String clusters) {
        ServiceInfo oldService = getServiceInfo0(serviceName, clusters);
        try {

            String result = serverProxy.queryList(serviceName, clusters, pushReceiver.getUDPPort(), false);

            if (StringUtils.isNotEmpty(result)) {
                processServiceJSON(result);
            }
        } catch (Exception e) {
            NAMING_LOGGER.error("[NA] failed to update serviceName: " + serviceName, e);
        } finally {
            if (oldService != null) {
                synchronized (oldService) {
                    oldService.notifyAll();
                }
            }
        }
    }

    private List<ServiceInfo> updateServiceNow(List<ServiceRefreshTimeModel> serviceRefList) {
        List<ServiceInfo> oldServiceList = new ArrayList<ServiceInfo>();
        List<Map<String, String>> serviceList = new ArrayList<Map<String, String>>();
        List<ServiceInfo> newServiceList = new ArrayList<ServiceInfo>();
        int len = serviceRefList.size();

        try {
            for (int index = 0; index < len; index++) {
                ServiceRefreshTimeModel model = serviceRefList.get(index);
                ServiceInfo oldService = getServiceInfo0(model.getServiceName(), model.getClusters());
                if (oldService != null) {
                    oldServiceList.add(oldService);
                }
                Map<String, String> serviceModel = new HashMap<String, String>();
                serviceModel.put(CommonParams.SERVICE_NAME, model.getServiceName());
                serviceModel.put("clusters", model.getClusters());
                serviceModel.put("udpPort", String.valueOf(pushReceiver.getUDPPort()));
                serviceModel.put("healthyOnly", Boolean.FALSE.toString());
                serviceList.add(serviceModel);
            }
            String result = serverProxy.queryMultilist(serviceList);

            if (StringUtils.isNotEmpty(result)) {
                JSONArray resultList = JSON.parseArray(result);
                len = resultList.size();
                for (int index = 0; index < len; index++) {
                    JSONObject objResult = resultList.getJSONObject(index);
                    ServiceInfo newServiceInfo = processServiceJSON(objResult.toJSONString());
                    newServiceList.add(newServiceInfo);
                }
            }
        } catch (Exception e) {
            NAMING_LOGGER.error("[NA] failed to batch update serviceName: " + serviceRefList.get(0).getServiceName(), e);
        } finally {
            if (!oldServiceList.isEmpty()) {
                len = oldServiceList.size();
                for (int index = 0; index < len; index++) {
                    ServiceInfo oldService = oldServiceList.get(index);
                    synchronized (oldService) {
                        oldService.notifyAll();
                    }
                }
            }
        }
        return newServiceList;
    }

    public class BatchUpdateTask implements Runnable {
        private List<ServiceRefreshTimeModel> serviceList = new ArrayList<ServiceRefreshTimeModel>();
        private Map<String, ServiceRefreshTimeModel> serviceNameMap = new HashMap<String, ServiceRefreshTimeModel>();
        private Lock lock = new ReentrantLock();

        public BatchUpdateTask() {
        }

        public boolean addService(String serviceName, String clusters) {
            String key = ServiceInfo.getKey(serviceName, clusters);
            if (serviceNameMap.containsKey(key)) {
                return true;
            }
            lock.lock();
            try {
                if (serviceNameMap.containsKey(key)) {
                    return true;
                }
                ServiceRefreshTimeModel serviceModel = new ServiceRefreshTimeModel(serviceName, clusters);
                serviceList.add(serviceModel);
                serviceNameMap.put(key, serviceModel);
            } finally {
                lock.unlock();
            }
            return true;
        }

        private void updateRefreshTime(List<ServiceInfo> newServiceList) {
            int len = newServiceList.size();
            for (int index = 0; index < len; index++) {
                ServiceInfo serviceInfo = newServiceList.get(index);
                String key = ServiceInfo.getKey(serviceInfo.getName() , serviceInfo.getClusters());
                ServiceRefreshTimeModel refreshTimeModel = serviceNameMap.get(key);
                if (serviceInfo.getLastRefTime() != 0) {
                    refreshTimeModel.setLastRefTime(serviceInfo.getLastRefTime());
                }
            }
        }

        @Override
        public void run() {
            int len = serviceList.size();
            try {

                List<ServiceRefreshTimeModel> serviceRefList = new ArrayList<ServiceRefreshTimeModel>();

                for (int index = 0; index < len; index++) {
                    ServiceRefreshTimeModel freshModel = serviceList.get(index);
                    String key = ServiceInfo.getKey(freshModel.getServiceName(), freshModel.getClusters());
                    ServiceInfo serviceObj = serviceInfoMap.get(key);
                    if (serviceObj == null || serviceObj.getLastRefTime() <= freshModel.getLastRefTime()) {
                        serviceRefList.add(freshModel);
                    }
                }
                if (serviceRefList.isEmpty()) {
                    executor.schedule(this, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
                } else {
                    List<ServiceInfo> newServiceList = updateServiceNow(serviceRefList);
                    if (newServiceList.isEmpty()) {
                        executor.schedule(this, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
                    } else {
                        long timeout = newServiceList.get(0).getCacheMillis();
                        if (timeout == 1000) {
                            timeout = DEFAULT_DELAY;
                        }
                        executor.schedule(this, timeout, TimeUnit.MILLISECONDS);
                        updateRefreshTime(newServiceList);
                    }
                }
            } catch (Throwable e) {
                NAMING_LOGGER.warn("[NA] failed to batch update serviceName ", e);
                executor.schedule(this, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
            }

        }
    }


}
