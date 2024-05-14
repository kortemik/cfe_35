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

import com.codahale.metrics.*;
import com.teragrep.cfe_35.config.RoutingConfig;
import com.teragrep.cfe_35.router.targets.DeadLetter;
import com.teragrep.cfe_35.router.targets.Inspection;
import com.teragrep.rlo_06.*;

import com.teragrep.rlp_01.RelpFrameTX;
import com.teragrep.rlp_03.channel.socket.TransportInfo;
import com.teragrep.rlp_03.frame.RelpFrame;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.frame.delegate.event.RelpEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.codahale.metrics.MetricRegistry.name;

public class MessageParser extends RelpEvent {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageParser.class);
    private final TargetRouting targetRouting;
    private final Timer responseLatency;
    private final Timer lookupLatency;
    private final Counter records;
    private final Counter bytes;
    private final Counter connections;
    private final RFC5424Frame rfc5424Frame;
    private TransportInfo transportInfo;

    private final CFE07RecordFrame cfe07RecordFrame;
    private final KIN02RecordFrame kin02RecordFrame;
    private final CFE16RecordFrame cfe16RecordFrame;

    final DeadLetter deadLetter;
    final Inspection inspection;

    MessageParser(
            RoutingLookup routingLookup,
            TargetRouting targetRouting,
            MetricRegistry metricRegistry,
            RoutingConfig routingConfig
    ) {
        this.targetRouting = targetRouting;
        this.deadLetter = new DeadLetter();
        this.inspection = new Inspection();

        this.responseLatency = metricRegistry
                .timer(name(MessageParser.class, "responseLatency"), () -> new Timer(new SlidingWindowReservoir(10000)));
        this.lookupLatency = metricRegistry
                .timer(name(MessageParser.class, "lookupLatency"), () -> new Timer(new SlidingWindowReservoir(10000)));

        this.records = metricRegistry.counter(name(MessageParser.class, "records"));
        this.bytes = metricRegistry.counter(name(MessageParser.class, "bytes"));

        this.connections = metricRegistry.counter("connections");
        this.connections.inc();

        this.rfc5424Frame = new RFC5424Frame();

        this.cfe07RecordFrame = new CFE07RecordFrame(routingLookup, rfc5424Frame, deadLetter, inspection);
        this.kin02RecordFrame = new KIN02RecordFrame(
                routingLookup,
                rfc5424Frame,
                routingConfig,
                deadLetter,
                inspection
        );
        this.cfe16RecordFrame = new CFE16RecordFrame(
                routingLookup,
                rfc5424Frame,
                routingConfig,
                deadLetter,
                inspection
        );

    }

    @Override
    public void accept(FrameContext frameContext) {
        transportInfo = frameContext.establishedContext().socket().getTransportInfo();
        byte[] payload = frameContext.relpFrame().payload().toBytes();

        List<CompletableFuture<RelpFrame>> transmitted = new ArrayList<>();
        try (final Timer.Context context = responseLatency.time()) {
            // increment counters
            bytes.inc(payload.length);
            records.inc();

            InputStream inputStream = new ByteArrayInputStream(payload);
            rfc5424Frame.load(inputStream);

            if (rfc5424Frame.next()) {
                final RoutingData routingData;
                try (Timer.Context lookupContext = lookupLatency.time()) {
                    if (kin02RecordFrame.validate()) {
                        routingData = kin02RecordFrame.route(payload);
                    }
                    else if (cfe16RecordFrame.validate()) {
                        routingData = cfe16RecordFrame.route(payload);
                    }
                    else {
                        routingData = cfe07RecordFrame.route(payload);
                    }
                }

                LOGGER.debug("about to route");
                transmitted.addAll(targetRouting.route(routingData));
                LOGGER.debug("routed");

            }
        }
        catch (Exception e) {
            LOGGER
                    .error(
                            "route to <inspection> because exception while handling data from <{}>:<{}>",
                            transportInfo.getPeerAddress(), transportInfo.getPeerPort(), e
                    );
            transmitted.addAll(targetRouting.route(new RoutingData(payload, Collections.singleton(inspection.name))));
        }

        if (transmitted.isEmpty()) {
            throw new IllegalStateException("no routing target futures were found");
        }

        CompletableFuture<RelpFrame>[] completableFuturesArrayTemplate = new CompletableFuture[0];

        CompletableFuture<RelpFrame>[] completableFutures = transmitted.toArray(completableFuturesArrayTemplate);

        // TODO create error handler, that re-creates the client when error occurs

        CompletableFuture.allOf(completableFutures).thenRun(() -> {
            LOGGER.debug("all transmitted.size() <{}> futures completed successfully", transmitted.size());
            // respond that it was processed ok
            String replyOk = "200 OK";
            int txn = frameContext.relpFrame().txn().toInt();
            RelpFrameTX relpFrameTX = new RelpFrameTX("rsp", replyOk.getBytes(StandardCharsets.UTF_8));
            relpFrameTX.setTransactionNumber(txn);
            frameContext.establishedContext().relpWrite().accept(Collections.singletonList(relpFrameTX));
            LOGGER.debug("replyOk <{}> for txn <{}>", replyOk, txn);
            frameContext.relpFrame().close();
        });
    }

    @Override
    public void close() {
        if (transportInfo != null) {
            LOGGER.debug("closing connection for <{}:{}>", transportInfo.getPeerAddress(), transportInfo.getPeerPort());
        }
        targetRouting.close();
        this.connections.dec();
    }

}
