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
import com.teragrep.rlp_03.frame.delegate.FrameDelegate;
import com.teragrep.net_01.server.ServerFactory;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EmptyTagTest {

    private final List<byte[]> spoolList = new ArrayList<>();
    private final List<byte[]> inspectionList = new ArrayList<>();
    private final List<byte[]> siem0List = new ArrayList<>();
    private final List<byte[]> hdfsList = new ArrayList<>();
    private final List<byte[]> deadLetterList = new ArrayList<>();

    private final int port = 13600;

    @BeforeAll
    public void setupTargets() throws IOException {
        setup(13601, spoolList);
        setup(13602, inspectionList);
        setup(13603, siem0List);
        setup(13604, hdfsList);
        setup(13605, deadLetterList);

        setupTestServer();
    }

    @AfterEach
    public void cleanTargets() {
        spoolList.clear();
        inspectionList.clear();
        siem0List.clear();
        hdfsList.clear();
        deadLetterList.clear();
    }

    private void setup(int port, List<byte[]> recordList) throws IOException {
        Consumer<FrameContext> cbFunction = relpFrameServerRX -> recordList
                .add(relpFrameServerRX.relpFrame().payload().toBytes());
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        EventLoopFactory eventLoopFactory = new EventLoopFactory();
        EventLoop eventLoop = eventLoopFactory.create(); // FIXME this is not cleaned up
        Thread eventLoopThread = new Thread(eventLoop);
        eventLoopThread.start(); // FIXME this is not cleaned up

        ServerFactory serverFactory = new ServerFactory(
                eventLoop,
                executorService,
                new PlainFactory(),
                new FrameDelegationClockFactory(() -> new DefaultFrameDelegate(cbFunction))
        );
        serverFactory.create(port);
        ;
    }

    private void setupTestServer() throws IOException {
        MetricRegistry metricRegistry = new MetricRegistry();
        System.setProperty("routingTargetsConfig", "src/test/resources/targetsEmptyTag.json");
        System.setProperty("cfe07LookupPath", "src/test/resources/cfe_07_empty");
        System.setProperty("cfe16LookupPath", "src/test/resources/cfe_16");
        System.setProperty("cfe16TruncationLength", "682");
        System.setProperty("kin02LookupPath", "src/test/resources/kin_02");
        System.setProperty("kin02TruncationLength", "245");
        RoutingConfig routingConfig = new RoutingConfig();
        RoutingLookup routingLookup = new RoutingLookup(routingConfig);

        EventLoopFactory eventLoopFactory = new EventLoopFactory();
        EventLoop eventLoop = eventLoopFactory.create(); // FIXME this is not cleaned up
        Thread eventLoopThread = new Thread(eventLoop);
        eventLoopThread.start(); // FIXME this is not cleaned up

        ExecutorService executorService = Executors.newFixedThreadPool(4); // FIXME this is not cleaned up

        ConnectContextFactory connectContextFactory = new ConnectContextFactory(executorService, new PlainFactory());
        RelpClientFactory clientFactory = new RelpClientFactory(connectContextFactory, eventLoop);

        Supplier<FrameDelegate> routingInstanceSupplier = () -> {
            TargetRouting targetRouting;
            try {
                targetRouting = new ParallelTargetRouting(routingConfig, metricRegistry, clientFactory);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            MessageParser messageParser = new MessageParser(
                    routingLookup,
                    targetRouting,
                    metricRegistry,
                    routingConfig
            );
            return new DefaultFrameDelegate(messageParser);
        };

        ServerFactory serverFactory = new ServerFactory(
                eventLoop,
                executorService,
                new PlainFactory(),
                new FrameDelegationClockFactory(routingInstanceSupplier)
        );
        serverFactory.create(port);
    }

    private void sendRecord(byte[] record) {
        try (Output output = new Output("test1", "localhost", port, 1000, 1000, 1000, 1000, new MetricRegistry())) {
            output.accept(record);
        }
    }

    @Test
    public void sendEmptyTag() {
        String msg = "<15>1 2023-10-03T15:00:52+03:00 empty-tag  - - - empty tag";
        sendRecord(msg.getBytes(StandardCharsets.UTF_8));
        // test that it goes to spool
        Assertions.assertEquals(msg, new String(spoolList.get(0), StandardCharsets.UTF_8));
    }

}
