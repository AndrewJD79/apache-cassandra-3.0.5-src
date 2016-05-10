package org.apache.cassandra.config;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class AcornOptions
{
    public String keyspace_prefix;

    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
