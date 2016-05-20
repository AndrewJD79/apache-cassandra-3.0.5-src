package org.apache.cassandra.config;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class AcornOptions
{
    public String keyspace_prefix;
    public long attr_pop_broadcast_interval_in_ms;
    public long attr_pop_monitor_window_size_in_ms;
    public boolean use_attr_user;
    public boolean use_attr_topic;
    public double extra_random_replicas_ratio;
    public String attr_filter_topic;
    public String attr_filter_user;

    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
