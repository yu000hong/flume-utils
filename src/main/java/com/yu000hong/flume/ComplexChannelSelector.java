package com.yu000hong.flume;

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.AbstractChannelSelector;

import java.util.*;

public class ComplexChannelSelector extends AbstractChannelSelector {

    public static final String CONFIG_HEADERS_NAME = "headers";
    public static final String CONFIG_PREFIX_MAPPING = "mapping.";
    public static final String CONFIG_DEFAULT_CHANNEL = "default";
    public static final String CONFIG_PREFIX_OPTIONAL = "optional.";

    private static final List<Channel> EMPTY_LIST =
        Collections.emptyList();

    private List<String> headerNames;

    private Map<String, List<Channel>> requiredChannelMapping;
    private Map<String, List<Channel>> optionalChannelMapping;
    private List<Channel> defaultChannels;

    @Override
    public List<Channel> getRequiredChannels(Event event) {
        List<String> headers = new ArrayList<>(headerNames.size());
        for (String headerName : headerNames) {
            String header = event.getHeaders().get(headerName);
            if (StringUtils.isBlank(header)) {
                break;
            } else {
                headers.add(header);
            }
        }
        while (!headers.isEmpty()) {
            String key = Joiner.on(".").join(headers);
            if (requiredChannelMapping.containsKey(key)) {
                return requiredChannelMapping.get(key);
            } else {
                headers.remove(headers.size() - 1);
            }
        }
        return defaultChannels;
    }

    @Override
    public List<Channel> getOptionalChannels(Event event) {
        List<String> headers = new ArrayList<>(headerNames.size());
        for (String headerName : headerNames) {
            String header = event.getHeaders().get(headerName);
            if (StringUtils.isBlank(header)) {
                break;
            } else {
                headers.add(header);
            }
        }
        while (!headers.isEmpty()) {
            String key = Joiner.on(".").join(headers);
            if (optionalChannelMapping.containsKey(key)) {
                return optionalChannelMapping.get(key);
            } else {
                headers.remove(headers.size() - 1);
            }
        }
        return EMPTY_LIST;
    }

    @Override
    public void configure(Context context) {
        this.headerNames = Arrays.asList(context.getString(CONFIG_HEADERS_NAME).split(" "));
        Map<String, Channel> channelNameMap = getChannelNameMap();
        //defaultChannels
        defaultChannels = getChannelListFromNames(
            context.getString(CONFIG_DEFAULT_CHANNEL), channelNameMap);
        //requiredChannelMapping
        Map<String, String> mapConfig =
            context.getSubProperties(CONFIG_PREFIX_MAPPING);
        requiredChannelMapping = new HashMap<>();
        for (String headerValue : mapConfig.keySet()) {
            List<Channel> configuredChannels = getChannelListFromNames(
                mapConfig.get(headerValue),
                channelNameMap);
            //This should not go to default channel(s)
            //because this seems to be a bad way to configure.
            if (configuredChannels.size() == 0) {
                throw new FlumeException("No channel configured for when "
                    + "header value is: " + headerValue);
            }
            if (requiredChannelMapping.put(headerValue, configuredChannels) != null) {
                throw new FlumeException("Selector channel configured twice");
            }
        }
        //optionalChannelMapping
        Map<String, String> optionalChannelsMapping =
            context.getSubProperties(CONFIG_PREFIX_OPTIONAL);
        optionalChannelMapping = new HashMap<>();
        for (String hdr : optionalChannelsMapping.keySet()) {
            List<Channel> confChannels = getChannelListFromNames(
                optionalChannelsMapping.get(hdr), channelNameMap);
            if (confChannels.isEmpty()) {
                confChannels = EMPTY_LIST;
            }
            //Remove channels from optional channels, which are already
            //configured to be required channels.
            List<Channel> requiredChannels = requiredChannelMapping.get(hdr);
            //Check if there are required channels, else defaults to default channels
            if (requiredChannels == null || requiredChannels.isEmpty()) {
                requiredChannels = defaultChannels;
            }
            for (Channel c : requiredChannels) {
                if (confChannels.contains(c)) {
                    confChannels.remove(c);
                }
            }
            if (optionalChannelMapping.put(hdr, confChannels) != null) {
                throw new FlumeException("Selector channel configured twice");
            }
        }
    }

}

