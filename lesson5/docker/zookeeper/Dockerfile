FROM confluentinc/cp-zookeeper:7.6.1

USER root
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar /usr/share/java/jmx_prometheus_javaagent.jar
RUN chmod 0644 /usr/share/java/jmx_prometheus_javaagent.jar
