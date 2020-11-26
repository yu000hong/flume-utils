package com.yu000hong.flume;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.twitter.finagle.common.zookeeper.ServerSet;
import com.twitter.finagle.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.finagle.common.zookeeper.ServerSetImpl;
import com.twitter.finagle.common.zookeeper.ZooKeeperClient;
import com.twitter.util.Duration;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusMetric implements MonitorService {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusMetric.class);

    private static final String CONF_ZK_SERVERS = "zkservers";
    private static final String CONF_ZK_PATH = "zkpath";
    private static final String CONF_PORT = "port";
    private static final String CONF_HOST = "host";

    private boolean zkEnabled = false;
    private String zkServers;
    private String zkPath;
    private String host;
    private Integer port;
    private HTTPServer server;

    private EndpointStatus status;
    private ZooKeeperClient zkClient;

    @Override
    public void start() {
        DefaultExports.initialize();
        new JmxExports().register();
        try {
            LOG.info("monitor server: {}:{}", host, port);
            server = new HTTPServer(host, port, true);
        } catch (IOException e) {
            LOG.error("Error when starting http server", e);
            throw new RuntimeException(e);
        }

        if (zkEnabled) {
            try {
                List<InetSocketAddress> servers = parseZkServers(zkServers);
                zkClient = new ZooKeeperClient(Duration.apply(1000, MILLISECONDS), servers);
                ServerSet serverSet = new ServerSetImpl(zkClient, zkPath);
                status = serverSet.join(new InetSocketAddress(host, port), new HashMap<>(0));
                LOG.info("serversets status: {}", status.toString());
            } catch (Exception e) {
                LOG.error("Error when registering in zookeeper", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                LOG.error("Error when stopping http server", e);
            }
        }
        if (status != null) {
            try {
                status.leave();
            } catch (Exception e) {
                LOG.error("Error when leaving serversets", e);
            }
        }
        if (zkClient != null) {
            try {
                zkClient.close();
            } catch (Exception e) {
                LOG.error("Error when closing zookeeper client", e);
            }
        }
    }

    @Override
    public void configure(Context context) {
        this.host = context.getString(CONF_HOST);
        if (this.host == null) {
            throw new ConfigurationException("Host cannot be null");
        }
        this.port = context.getInteger(CONF_PORT);
        if (this.port == null) {
            throw new ConfigurationException("Port cannot be null");
        }
        this.zkServers = context.getString(CONF_ZK_SERVERS);
        this.zkPath = context.getString(CONF_ZK_PATH);
        if (this.zkServers != null && this.zkPath != null) {
            zkEnabled = true;
        } else {
            if (this.zkServers != null || this.zkPath != null) {
                throw new ConfigurationException(
                    "zkservers and zkpath must be both provided or neither");
            }
        }
    }

    private static List<InetSocketAddress> parseZkServers(String zkServers) {
        String[] servers = zkServers.split(",");
        return Arrays.stream(servers).map(server -> {
            String[] parts = server.split(":");
            return new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
        }).collect(Collectors.toList());
    }

    private static class JmxExports extends Collector {

        @Override
        public List<MetricFamilySamples> collect() {
            List<MetricFamilySamples> mfs = new ArrayList<>();
            Map<String, Map<String, String>> metricsMap = JMXPollUtil.getAllMBeans();
            for (String typeAndName : metricsMap.keySet()) {
                String[] parts = typeAndName.split("\\.");
                String type = parts[0];
                String name = parts[1];
                Map<String, String> metrics = metricsMap.get(typeAndName);
                for (String key : metrics.keySet()) {
                    String value = metrics.get(key);
                    MetricFamilySamples sample = process(type, name, key, value);
                    if (sample != null) {
                        mfs.add(sample);
                    }
                }
            }
            return mfs;
        }

        private MetricFamilySamples process(String type, String name, String key, String value) {
            MetricType metricType = getMetricType(type, key);
            if (metricType == MetricType.NONE) {
                return null;
            }
            List<String> labels = Arrays.asList("type", "name");
            List<String> labelValues = Arrays.asList(type, name);
            double v = Double.parseDouble(value);
            String metric = "FLUME_" + type + "_" + key;
            String help = type + " " + key;
            if (metricType == MetricType.GAUGE) {
                GaugeMetricFamily gauge = new GaugeMetricFamily(metric, help, labels);
                gauge.addMetric(labelValues, v);
                return gauge;
            } else {
                CounterMetricFamily counter = new CounterMetricFamily(metric, help, labels);
                counter.addMetric(labelValues, v);
                return counter;
            }
        }


        private MetricType getMetricType(String type, String metric) {
            List<String> noneMetrics = Arrays.asList("Type", "StartTime", "StopTime");
            List<String> channelGauges = Arrays.asList(
                "ChannelCapacity", "ChannelFillPercentage", "ChannelSize");
            if (noneMetrics.contains(metric)) {
                return MetricType.NONE;
            }
            if ("CHANNEL".equals(type) && channelGauges.contains(metric)) {
                return MetricType.GAUGE;
            } else {
                return MetricType.COUNTER;
            }
        }

    }

    private enum MetricType {
        NONE, GAUGE, COUNTER
    }

}
