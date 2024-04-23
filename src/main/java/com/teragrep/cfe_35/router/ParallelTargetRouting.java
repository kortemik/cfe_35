/*
 * Java Record Router CFE-35
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.cfe_35.router;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.teragrep.cfe_35.config.RoutingConfig;
import com.teragrep.cfe_35.config.json.TargetConfig;
import com.teragrep.rlp_03.client.Client;
import com.teragrep.rlp_03.client.ClientFactory;
import com.teragrep.rlp_03.frame.RelpFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static com.codahale.metrics.MetricRegistry.name;

public class ParallelTargetRouting implements TargetRouting {

    public static final Logger LOGGER = LoggerFactory.getLogger(ParallelTargetRouting.class);

    private final Map<String, Client> outputMap = new HashMap<>();
    private final Counter totalRecords;
    private final Counter totalBytes;
    private final RoutingConfig routingConfig;
    private final ClientFactory clientFactory;

    public ParallelTargetRouting(
            RoutingConfig routingConfig,
            MetricRegistry metricRegistry,
            ClientFactory clientFactory
    ) throws IOException {
        this.routingConfig = routingConfig;
        this.clientFactory = clientFactory;

        this.totalRecords = metricRegistry.counter(name(ParallelTargetRouting.class, "totalRecords"));
        this.totalBytes = metricRegistry.counter(name(ParallelTargetRouting.class, "totalBytes"));

    }

    private Map<String, Client> createClients() {
        Map<String, Client> outputs = new HashMap<>();
        Map<String, TargetConfig> configMap = routingConfig.getTargetConfigMap();

        for (Map.Entry<String, TargetConfig> entry : configMap.entrySet()) {
            String targetName = entry.getKey();
            TargetConfig targetConfig = entry.getValue();
            if (targetConfig.isEnabled()) {
                String hostname = targetConfig.getTarget();
                int port = Integer.parseInt(targetConfig.getPort());
                int connectionTimeout = routingConfig.getConnectionTimeout();
                // FIXME blocked ~ you ran it in the constructor not in the method
                InetSocketAddress isa = new InetSocketAddress(hostname, port);
                // TODO count connectLatency
                try {
                    LOGGER.info("opening client to isa <[{}]>", isa);
                    Client client = clientFactory.open(isa, connectionTimeout, TimeUnit.MILLISECONDS);

                    String offer = ("\nrelp_version=0\nrelp_software=cfe_35\ncommands=" + "syslog" + "\n");
                    LOGGER.info("transmitting offer <{}>", offer);
                    CompletableFuture<RelpFrame> openFuture = client
                            .transmit("open", offer.getBytes(StandardCharsets.US_ASCII));

                    LOGGER.info("waiting offer reply");
                    RelpFrame openResponse = openFuture.get();
                    LOGGER.info("got openResponse <[{}]>", openResponse);
                    if (!openResponse.payload().toString().startsWith("200 OK")) {
                        throw new IllegalStateException("open response for client not 200 OK");
                    }
                    outputs.put(targetName, client);
                    LOGGER.info("created client for isa <[{}]>", isa);
                }
                catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
                    // TODO what about these?
                    throw new RuntimeException(e);
                }
            }
        }
        return outputs;
    }

    public List<CompletableFuture<RelpFrame>> route(RoutingData routingData) {
        if (outputMap.isEmpty()) {
            outputMap.putAll(createClients());
        }

        List<CompletableFuture<RelpFrame>> outputReplyFutures = new ArrayList<>(outputMap.size());

        for (String target : routingData.targets) {
            Client client = outputMap.get(target);
            if (client == null) {
                throw new IllegalArgumentException("no such target <[" + target + "]>");
            }

            CompletableFuture<RelpFrame> transmitted = client.transmit("syslog", routingData.payload);
            outputReplyFutures.add(transmitted);

            totalRecords.inc();
            totalBytes.inc(routingData.payload.length);
        }
        LOGGER.info("returning outputReplyFutures.size() <{}>", outputReplyFutures.size());
        return outputReplyFutures;
    }

    @Override
    public void close() {
        for (Client client : outputMap.values()) {
            client.transmit("close", "".getBytes(StandardCharsets.UTF_8));
            client.close();
        }
    }

}
