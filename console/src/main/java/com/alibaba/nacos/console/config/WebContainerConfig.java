package com.alibaba.nacos.console.config;

import com.alibaba.nacos.core.utils.PropertyUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.alibaba.nacos.core.utils.Constants.IP_ADDRESS;
import static com.alibaba.nacos.core.utils.Constants.NACOS_SERVER_IP;

@Component
public class WebContainerConfig implements WebServerFactoryCustomizer<ConfigurableWebServerFactory> {
    private final Logger log = LoggerFactory.getLogger(WebContainerConfig.class);

    @Override
    public void customize(ConfigurableWebServerFactory factory) {
        String nacosIp = System.getProperty(NACOS_SERVER_IP);
        if (StringUtils.isBlank(nacosIp)) {
            nacosIp = PropertyUtil.getProperty(NACOS_SERVER_IP);
        }
        if (StringUtils.isBlank(nacosIp)) {
            nacosIp = PropertyUtil.getProperty(IP_ADDRESS);
        }
        if (StringUtils.isBlank(nacosIp)) {
            nacosIp = PropertyUtil.getProperty(IP_ADDRESS);
        }
        if (!StringUtils.isBlank(nacosIp)) {
            try {
                InetAddress bindAddress = InetAddress.getByName(nacosIp);
                factory.setAddress(bindAddress);
            } catch (UnknownHostException e) {
                //ignore it.
                log.warn("Unable to parse ip address (" + nacosIp + ")");
            }
        }
    }
}
