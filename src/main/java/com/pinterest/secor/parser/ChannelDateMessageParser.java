package com.pinterest.secor.parser;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

/**
 * ChannelDateMessageParser extracts timestamp field (specified by
 * 'message.timestamp.name') and the date pattern (specified by
 * 'message.timestamp.input.pattern'). The output file pattern is fetched from
 * the property 'secor.partition.output_dt_format'.
 * 
 * This generic class can even handle the DateMessageParse functionality. For
 * ex: it will generate the same partition when the
 * 'secor.partition.output_dt_format' property is set to "'dt='yyyy-MM-dd"
 * 
 * @see http://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html
 * 
 * @author Santhosh Vasabhaktula (santhosh.vasabhaktula@gmail.com)
 * 
 */
public class ChannelDateMessageParser extends MessageParser {

	private static final Logger LOG = LoggerFactory.getLogger(PatternDateMessageParser.class);
	protected static final String defaultDate = "1970-01-01";
	protected static final String defaultFormatter = "yyyy-MM-dd";
	private Map<String, String> partitionPrefixMap;
	private static final String channelScrubRegex = "[^a-zA-Z0-9._$-]";
	private SimpleDateFormat outputFormatter;

	public ChannelDateMessageParser(SecorConfig config) {
		super(config);
		outputFormatter = new SimpleDateFormat(
				StringUtils.defaultIfBlank(mConfig.getPartitionOutputDtFormat(), defaultFormatter));
		partitionPrefixMap = new HashMap<>();
		String partitionMapping = config.getPartitionPrefixMapping();
		if (null != partitionMapping) {
			JSONObject jsonObject = (JSONObject) JSONValue.parse(partitionMapping);
			for (Entry<String, Object> entry : jsonObject.entrySet()) {
				partitionPrefixMap.put(entry.getKey(), entry.getValue().toString() + "/");
			}
		}
	}

	@Override
	public String[] extractPartitions(Message message) {

		JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
		boolean prefixEnabled = mConfig.isPartitionPrefixEnabled();
		String result[] = {defaultDate};

		if (jsonObject != null) {

			Object fieldValue = this.getTimeStamp(jsonObject, mConfig.getMessageTimestampName());
			if (fieldValue == null)
				fieldValue = this.getTimeStamp(jsonObject, mConfig.getFallbackMessageTimestampName());
			if (fieldValue == null)
				fieldValue = System.currentTimeMillis();

			Object eventValue = jsonObject.get(mConfig.getPartitionPrefixIdentifier());
			Object inputPattern = mConfig.getMessageTimestampInputPattern();
			if (inputPattern != null) {
				try {
					/*
					SimpleDateFormat outputFormatter = new SimpleDateFormat(
							StringUtils.defaultIfBlank(mConfig.getPartitionOutputDtFormat(), defaultFormatter));
					*/
					Date dateFormat = null;
					if (fieldValue instanceof Number) {
						dateFormat = new Date(((Number) fieldValue).longValue());
					} else if(fieldValue instanceof String) {
						SimpleDateFormat inputFormatter = new SimpleDateFormat(inputPattern.toString());
						dateFormat = inputFormatter.parse(fieldValue.toString());
					}
					if (dateFormat == null) {
                        LOG.error("Invalid timestamp value: {}", fieldValue);
						return result; // Return default if parsing fails
					}
					
					String channel = getChannel(jsonObject);

					String path = channel + "/";
					result[0] = prefixEnabled
							? getPrefix(eventValue.toString()) + path + outputFormatter.format(dateFormat)
							: path + outputFormatter.format(dateFormat);
					return result;
				} catch (Exception e) {
					e.printStackTrace();
					LOG.warn("Unable to get path: " + e.getMessage() + " - " + message.getPayload());
				}
			}
		}

		return result;
	}

	private String getPrefix(String prefixIdentifier) {
		String prefix = partitionPrefixMap.get(prefixIdentifier);
		if (StringUtils.isBlank(prefix)) {
			if (prefixIdentifier.contains("ME_")) {
				prefix = "others/";
			} else {
				prefix = partitionPrefixMap.get("DEFAULT");
			}
		}
		return prefix;
	}
	
	private Object getTimeStamp(JSONObject jsonObject, String timestampKey) {
		if (jsonObject == null) {
			LOG.error("Input JSON object is null. Cannot extract timestamp.");
			return null;
		}
		String[] timeStampKeys = timestampKey.split("\\.");
		if (timeStampKeys.length == 0) {
            LOG.warn("No timestamp keys provided in the configuration: {}", timestampKey);
			return null;
		}
		String jsonPath = "$." + String.join(".", timeStampKeys);
		Object timeStampValue = null;
		try {
			timeStampValue = JsonPath.parse(jsonObject.toString()).read(jsonPath, Object.class);
		} catch (PathNotFoundException e) {
            LOG.warn("Path not found for key: {}, Error: {}", timestampKey, e.getMessage());
		} catch (Exception e) {
            LOG.error("Unexpected error while parsing JSON for key: {}", timestampKey, e);
		}
		return timeStampValue;
	}

	private String getChannel(JSONObject jsonObject) {
		String channelStr = "";
		String finalChannelStr = "";
		String[] channelIdentifier = mConfig.getMessageChannelIdentifier();
		if (channelIdentifier.length > 0) {
			try {
				// If channel identifier is common for both raw and summary events
				if (channelIdentifier.length == 1) {
					channelStr = JsonPath.parse(jsonObject).read("$." + channelIdentifier[0], String.class);
				}
				// If channel identifier is different for raw and summary events. Ex: context.channel/dimensions.channel
				else {
					try {
						channelStr = JsonPath.parse(jsonObject).read("$." + channelIdentifier[0], String.class);
					}
					catch (PathNotFoundException e) {
						LOG.warn("Unable to get path: " + e.getMessage());
						channelStr = JsonPath.parse(jsonObject).read("$." + channelIdentifier[1], String.class);
					}
				}
			} catch (PathNotFoundException e) {
				LOG.warn("Unable to get path: " + e.getMessage());
				channelStr = "others";
			}
		}
		if (channelStr.isEmpty()) finalChannelStr = "others"; else finalChannelStr = channelStr;
		return finalChannelStr.replaceAll(channelScrubRegex, "");
	}
}