# flume-utils

some utils for flume

- LoggerSink
- PrometheusMetric

## PrometheusMetric

PrometheusMetric 使用prometheus-client将Flume自身收集的指标暴露出来，
由Prometheus服务器定时来Scrape。

使用方式：

```bash
bin/flume-ng agent --conf-file flume.conf \
 -Dflume.monitoring.type=com.yu000hong.flume.PrometheusMetric \
 -Dflume.monitoring.host=127.0.0.1 \
 -Dflume.monitoring.port=8888 \
 -Dflume.monitoring.zkservers=127.0.0.1:2181,127.0.0.1:2182 \
 -Dflume.monitoring.zkpath=/serversets/flume
```

其中，`flume.monitoring.zkservers`和`flume.monitoring.zkpath`是可选参数，
如果需要使用serversets自动收集指标的话，那么就需要配置这两个参数选项。



