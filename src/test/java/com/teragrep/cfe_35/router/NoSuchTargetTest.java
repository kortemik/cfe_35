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

import com.codahale.metrics.MetricRegistry;
import com.teragrep.cfe_35.config.RoutingConfig;

import com.teragrep.net_01.channel.context.ConnectContextFactory;
import com.teragrep.net_01.channel.socket.PlainFactory;
import com.teragrep.rlp_03.client.RelpClientFactory;
import com.teragrep.net_01.eventloop.EventLoop;
import com.teragrep.net_01.eventloop.EventLoopFactory;
import com.teragrep.rlp_03.frame.FrameDelegationClockFactory;
import com.teragrep.rlp_03.frame.delegate.DefaultFrameDelegate;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.net_01.server.ServerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NoSuchTargetTest {

    private final List<byte[]> spoolList = new ArrayList<>();
    private final List<byte[]> inspectionList = new ArrayList<>();
    private final List<byte[]> siem0List = new ArrayList<>();
    private final List<byte[]> hdfsList = new ArrayList<>();
    private final List<byte[]> deadLetterList = new ArrayList<>();
    private final MetricRegistry metricRegistry = new MetricRegistry();

    @BeforeAll
    public void setupTargets() throws IOException {
        setup(6601, spoolList);
        setup(6602, inspectionList);
        setup(6603, siem0List);
        setup(6604, hdfsList);
        setup(6605, deadLetterList);
    }

    private void setup(int port, List<byte[]> recordList) throws IOException {
        Consumer<FrameContext> cbFunction = relpFrameServerRX -> recordList
                .add(relpFrameServerRX.relpFrame().payload().toBytes());

        EventLoopFactory eventLoopFactory = new EventLoopFactory();
        EventLoop eventLoop = eventLoopFactory.create(); // FIXME this is not cleaned up
        Thread eventLoopThread = new Thread(eventLoop);
        eventLoopThread.start(); // FIXME this is not cleaned up

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        ServerFactory serverFactory = new ServerFactory(
                eventLoop,
                executorService,
                new PlainFactory(),
                new FrameDelegationClockFactory(() -> new DefaultFrameDelegate(cbFunction))
        );
        serverFactory.create(port);
    }

    @Test
    public void noSuchTargetTest() throws IOException {
        System.setProperty("routingTargetsConfig", "src/test/resources/targetsNoSuchTarget.json");
        RoutingConfig routingConfig = new RoutingConfig();

        EventLoopFactory eventLoopFactory = new EventLoopFactory();
        EventLoop eventLoop = eventLoopFactory.create(); // FIXME this is not cleaned up
        Thread eventLoopThread = new Thread(eventLoop);
        eventLoopThread.start(); // FIXME this is not cleaned up

        ExecutorService executorService = Executors.newFixedThreadPool(4); // FIXME this is not cleaned up

        ConnectContextFactory connectContextFactory = new ConnectContextFactory(executorService, new PlainFactory());
        RelpClientFactory clientFactory = new RelpClientFactory(connectContextFactory, eventLoop);

        try (
                TargetRouting targetRouting = new ParallelTargetRouting(
                        routingConfig,
                        this.metricRegistry,
                        clientFactory
                )
        ) {
            Assertions
                    .assertThrows(
                            IllegalArgumentException.class, () -> targetRouting
                                    .route(new RoutingData("test1".getBytes(StandardCharsets.UTF_8), Collections.singleton("noSuchTarget")))
                    );
        }
    }
}
