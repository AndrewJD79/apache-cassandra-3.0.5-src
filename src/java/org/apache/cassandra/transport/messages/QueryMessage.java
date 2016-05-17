/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.transport.messages;

import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A CQL query
 */
public class QueryMessage extends Message.Request
{
    public static final Message.Codec<QueryMessage> codec = new Message.Codec<QueryMessage>()
    {
        public QueryMessage decode(ByteBuf body, int version)
        {
            String query = CBUtil.readLongString(body);
            return new QueryMessage(query, QueryOptions.codec.decode(body, version));
        }

        public void encode(QueryMessage msg, ByteBuf dest, int version)
        {
            CBUtil.writeLongString(msg.query, dest);
            if (version == 1)
                CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
            else
                QueryOptions.codec.encode(msg.options, dest, version);
        }

        public int encodedSize(QueryMessage msg, int version)
        {
            int size = CBUtil.sizeOfLongString(msg.query);

            if (version == 1)
            {
                size += CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency());
            }
            else
            {
                size += QueryOptions.codec.encodedSize(msg.options, version);
            }
            return size;
        }
    };

    public final String query;
    public final QueryOptions options;

    public QueryMessage(String query, QueryOptions options)
    {
        super(Type.QUERY);
        this.query = query;
        this.options = options;
    }

    public Message.Response execute(QueryState state)
    {
        try
        {
            if (options.getPageSize() == 0)
                throw new ProtocolException("The page size cannot be 0");

            UUID tracingId = null;
            if (isTracingRequested())
            {
                tracingId = UUIDGen.getTimeUUID();
                state.prepareTracingSession(tracingId);
            }

            if (state.traceNextQuery())
            {
                state.createTracingSession();

                ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                builder.put("query", query);
                if (options.getPageSize() > 0)
                    builder.put("page_size", Integer.toString(options.getPageSize()));
                if(options.getConsistency() != null)
                    builder.put("consistency_level", options.getConsistency().name());
                if(options.getSerialConsistency() != null)
                    builder.put("serial_consistency_level", options.getSerialConsistency().name());

                Tracing.instance.begin("Execute CQL3 query", state.getClientAddress(), builder.build());
            }

            // Looks like all queries, including system queries like this, go
            // through here.
            //   SELECT * FROM system_schema.keyspaces
            final String acorn_ks_regex = String.format(".* %s.*_pr\\..*", DatabaseDescriptor.getAcornOptions().keyspace_prefix);
            boolean acorn_pr = query.matches(acorn_ks_regex);
            if (acorn_pr) {
                // state is of type org.apache.cassandra.service.QueryState
                // options is of type org.apache.cassandra.cql3.QueryOptions$DefaultQueryOptions
                // getCustomPayload()=null
                //
                // You can get CL from options.getConsistency(). You can set it
                // from the constructor.
                //
                // This gives you whether it is an internal call or not:
                // ClientState cs = state.getClientState();
                //
                //logger.warn("Acorn: query={} state={} state.getClientState()={}"
                //        + " options={}"
                //        + " options.getConsistency()={}"
                //        + " options.getValues()={}"
                //        + " options.skipMetadata()={}"
                //        + " options.getProtocolVersion()={}"
                //        + " getCustomPayload()={}"
                //        , query
                //        , state, state.getClientState()
                //        , options
                //        , options.getConsistency()
                //        , options.getValues()
                //        , options.skipMetadata()
                //        , options.getProtocolVersion()
                //        , getCustomPayload());
                //
                logger.warn("Acorn: query={} options.getConsistency()={}"
                        , query
                        , options.getConsistency());
            }
            Message.Response response = ClientState.getCQLQueryHandler().process(acorn_pr, query, state, options, getCustomPayload());
            // response is of type ResultMessage$Rows

            //if (acorn_pr) {
            //    //logger.warn("Acorn: response={} {}", response, response.getClass().getName());
            //    if (response.getClass().equals(ResultMessage.Rows.class)) {
            //        ResultMessage.Rows r = (ResultMessage.Rows) response;
            //        // r.result is of type org.apache.cassandra.cql3.ResultSet
            //        logger.warn("Acorn: response.result={} {}", r.result, r.result.getClass().getName());
            //    }
            //}

            if (options.skipMetadata() && response instanceof ResultMessage.Rows)
                ((ResultMessage.Rows)response).result.metadata.setSkipMetadata();

            if (tracingId != null)
                response.setTracingId(tracingId);

            return response;
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            if (!((e instanceof RequestValidationException) || (e instanceof RequestExecutionException)))
                logger.error("Unexpected error during query", e);
            return ErrorMessage.fromException(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    @Override
    public String toString()
    {
        return "QUERY " + query + "[pageSize = " + options.getPageSize() + "]";
    }
}
