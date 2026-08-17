package com.pinterest.secor.monitoring;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.monitoring.PrometheusHandler;
import com.sun.net.httpserver.HttpExchange;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.LegacyListDelimiterHandler;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PrometheusTest {

    @Test
    public void testPrometheusIntegration() throws IOException {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setListDelimiterHandler(new LegacyListDelimiterHandler(','));
        properties.addProperty("secor.monitoring.metrics.collector.micrometer.prometheus.enabled", true);
        SecorConfig config = new SecorConfig(properties);
        MetricCollector collector = new MicroMeterMetricCollector();
        collector.initialize(config);

        final List<String> responses = new ArrayList<>();
        PrometheusHandler handler = new PrometheusHandler() {
            @Override
            public void render(String body, HttpExchange exchange, int code) {
                responses.add(body);
            }
        };
        HttpExchange exchange = mock(HttpExchange.class);

        collector.gauge("test", 1, "topic");

        handler.handle(exchange);
        // The Prometheus Java client 1.x exposition format (used by micrometer-registry-prometheus
        // 1.13+) no longer emits a trailing comma after the last label, unlike the older simpleclient
        // 0.x format which produced test{topic="topic",} 1.0
        assertTrue(responses.get(0).contains("test{topic=\"topic\"} 1.0"));
    }

}
