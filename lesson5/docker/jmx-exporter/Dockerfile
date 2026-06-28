FROM eclipse-temurin:17-jre

ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_httpserver/0.20.0/jmx_prometheus_httpserver-0.20.0.jar /opt/jmx_exporter/jmx_prometheus_httpserver.jar
RUN chmod 0644 /opt/jmx_exporter/jmx_prometheus_httpserver.jar

ENTRYPOINT ["java", "-jar", "/opt/jmx_exporter/jmx_prometheus_httpserver.jar"]
