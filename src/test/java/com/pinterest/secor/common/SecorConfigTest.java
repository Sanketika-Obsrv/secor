/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pinterest.secor.common;

import com.pinterest.secor.protobuf.Messages.UnitTestMessage1;
import com.pinterest.secor.protobuf.Messages.UnitTestMessage2;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SecorConfigTest {

    private PropertiesConfiguration loadProperties(String resourceName) throws ConfigurationException {
        URL configFile = Thread.currentThread().getContextClassLoader().getResource(resourceName);
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
                new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                        .configure(params.properties()
                                .setFileName(configFile.getPath()));
        return builder.getConfiguration();
    }

    @Test
    public void config_should_read_migration_required_properties_default_values() throws Exception {
        PropertiesConfiguration properties = loadProperties("secor.common.properties");

        SecorConfig secorConfig = new SecorConfig(properties);
        assertEquals("true", secorConfig.getDualCommitEnabled());
        assertEquals("kafka", secorConfig.getOffsetsStorage());
    }

    @Test
    public void config_should_read_migration_required() throws Exception {
        PropertiesConfiguration properties = loadProperties("secor.kafka.migration.test.properties");

        SecorConfig secorConfig = new SecorConfig(properties);
        assertEquals("false", secorConfig.getDualCommitEnabled());
        assertEquals("kafka", secorConfig.getOffsetsStorage());
    }

    @Test
    public void testProtobufMessageClassPerTopic() throws Exception {
        PropertiesConfiguration properties = loadProperties("secor.test.protobuf.properties");

        SecorConfig secorConfig = new SecorConfig(properties);
        Map<String, String> messageClassPerTopic = secorConfig.getProtobufMessageClassPerTopic();
        
        assertEquals(2, messageClassPerTopic.size());
        assertEquals(UnitTestMessage1.class.getName(), messageClassPerTopic.get("mytopic1"));
        assertEquals(UnitTestMessage2.class.getName(), messageClassPerTopic.get("mytopic2"));
    }

    @Test
    public void shouldReadMetricCollectorConfiguration() throws Exception {
        PropertiesConfiguration properties = loadProperties("secor.test.monitoring.properties");

        SecorConfig secorConfig = new SecorConfig(properties);

        assertEquals("com.pinterest.secor.monitoring.OstrichMetricCollector", secorConfig.getMetricsCollectorClass());
    }

    @Test
    public void shouldCheckIfConfigurationExists() throws Exception {
        PropertiesConfiguration properties = loadProperties("secor.test1.partition.properties");

        SecorConfig secorConfig = new SecorConfig(properties);

        assertEquals(false, secorConfig.checkPropertyProvided("ostrich.port"));
        assertEquals(true, secorConfig.checkPropertyProvided("secor.kafka.group"));
    }
}
