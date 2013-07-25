flume-timestamp-interceptor
===========================

Flume-NG timestamp interceptor. Parses delimited logs,  extracts formatted timestamp value,  and sets it as a header.



How to install
===========================

1. Download zipped source.
2. Unzip and go into the flume-timestamp-interceptor-master directory.
3. $ mvn package
4. Go into target directory and copy flume-timestamp-interceptor-1.2.0.jar into FLUME_HOME/lib.


How to configure
===========================

```text
agent.sources.tail-source.interceptors = timestamp
agent.sources.tail-source.interceptors.timestamp.type = org.apache.flume.interceptor.EventTimestampInterceptor$Builder
agent.sources.tail-source.interceptors.timestamp.preserveExisting = false
agent.sources.tail-source.interceptors.timestamp.delimiter = |
agent.sources.tail-source.interceptors.timestamp.dateIndex = 0
agent.sources.tail-source.interceptors.timestamp.dateFormat = yyyy-MM-dd HH:mm:ss
```

NOTE: index starts from 0
