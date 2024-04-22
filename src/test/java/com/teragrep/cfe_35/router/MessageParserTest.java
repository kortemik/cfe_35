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

import com.teragrep.rlp_03.channel.context.ConnectContextFactory;
import com.teragrep.rlp_03.channel.socket.PlainFactory;
import com.teragrep.rlp_03.client.ClientFactory;
import com.teragrep.rlp_03.eventloop.EventLoop;
import com.teragrep.rlp_03.eventloop.EventLoopFactory;
import com.teragrep.rlp_03.frame.delegate.DefaultFrameDelegate;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.server.ServerFactory;
import com.teragrep.rlp_03.frame.delegate.FrameDelegate;
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
public class MessageParserTest {

    private final List<byte[]> spoolList = new ArrayList<>();
    private final List<byte[]> inspectionList = new ArrayList<>();
    private final List<byte[]> siem0List = new ArrayList<>();
    private final List<byte[]> hdfsList = new ArrayList<>();
    private final List<byte[]> deadLetterList = new ArrayList<>();

    private final int port = 3600;

    @BeforeAll
    public void setupTargets() throws IOException {
        setup(3601, spoolList);
        setup(3602, inspectionList);
        setup(3603, siem0List);
        setup(3604, hdfsList);
        setup(3605, deadLetterList);

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

        EventLoopFactory eventLoopFactory = new EventLoopFactory();
        EventLoop eventLoop = eventLoopFactory.create(); // FIXME this is not cleaned up
        Thread eventLoopThread = new Thread(eventLoop);
        eventLoopThread.start(); // FIXME this is not cleaned up

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        ServerFactory serverFactory = new ServerFactory(
                eventLoop,
                executorService,
                new PlainFactory(),
                () -> new DefaultFrameDelegate(cbFunction)
        );
        serverFactory.create(port);
    }

    private void setupTestServer() throws IOException {
        MetricRegistry metricRegistry = new MetricRegistry();
        System.setProperty("routingTargetsConfig", "src/test/resources/targetsMessageParserTest.json");
        System.setProperty("cfe07LookupPath", "src/test/resources/cfe_07");
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
        ClientFactory clientFactory = new ClientFactory(connectContextFactory, eventLoop);

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
                routingInstanceSupplier
        );
        serverFactory.create(port);

    }

    private void sendRecord(byte[] record) {
        try (Output output = new Output("test1", "localhost", port, 1000, 1000, 1000, 1000, new MetricRegistry())) {
            output.accept(record);
        }
    }

    @Test
    public void sendInspectionTest() {
        sendRecord("test".getBytes(StandardCharsets.UTF_8));

        // test that it goes to inspection
        Assertions.assertEquals("test", new String(inspectionList.get(0), StandardCharsets.UTF_8));
    }

    @Test
    public void hostnameInvalidTest() {
        sendRecord(("<999>1    - - -").getBytes(StandardCharsets.UTF_8));

        // test that it goes to inspection
        Assertions.assertEquals("<999>1    - - -", new String(inspectionList.get(0), StandardCharsets.UTF_8));
    }

    @Test
    public void sendKin02SpoolTest() {
        final String record = "<14>1 2023-08-04T20:16:59.292Z aaa-bbb-test 578f2f4c-/bbb/test/bbb-front - - [stream-processor@48577 log-group=\"/example/logGroupName/ThatExists\" log-stream=\"task/bbb-front-service/a4b046968c23af470b6cf9db016d4583\" account=\"1234567890\"] Example";
        final String modifiedRecord = "<14>1 2023-08-04T20:16:59.292Z 1234567890.host.example.com exampleAppName - - [stream-processor@48577 log-group=\"/example/logGroupName/ThatExists\" log-stream=\"task/bbb-front-service/a4b046968c23af470b6cf9db016d4583\" account=\"1234567890\"] Example";
        sendRecord(record.getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals(modifiedRecord, new String(spoolList.get(0), StandardCharsets.UTF_8));
    }

    @Test
    public void sendCfe16SpoolTest() {
        final String record = "<14>1 2023-08-07T08:39:43.196Z CFE-16 capsulated - - [CFE-16-metadata@48577 authentication_token=\"My RoutingKey having token\" channel=\"defaultchannel\" time_source=\"generated\"][CFE-16-origin@48577 X-Forwarded-For=\"127.0.0.3\" X-Forwarded-Host=\"127.0.0.2\" X-Forwarded-Proto=\"http\"][event_id@48577 hostname=\"relay.example.com\" uuid=\"029EF30A9CB94D32BE40D3DCD01765AA\" unixtime=\"1691408383\" id_source=\"relay\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"relay.example.com\" source=\"localhost\" source_module=\"imptcp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"relay.example.com\" version_source=\"relay\"] \"Testing\"";
        final String modifiedRecord = "<14>1 2023-08-07T08:39:43.196Z my-routingkey-having-hostname.example.com capsulated - - [CFE-16-metadata@48577 authentication_token=\"My RoutingKey having token\" channel=\"defaultchannel\" time_source=\"generated\"][CFE-16-origin@48577 X-Forwarded-For=\"127.0.0.3\" X-Forwarded-Host=\"127.0.0.2\" X-Forwarded-Proto=\"http\"][event_id@48577 hostname=\"relay.example.com\" uuid=\"029EF30A9CB94D32BE40D3DCD01765AA\" unixtime=\"1691408383\" id_source=\"relay\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"relay.example.com\" source=\"localhost\" source_module=\"imptcp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"relay.example.com\" version_source=\"relay\"] \"Testing\"";
        sendRecord(record.getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals(modifiedRecord, new String(spoolList.get(0), StandardCharsets.UTF_8));
    }

    @Test
    public void sendKin02SpoolTruncatedTest() {
        final String record = "<14>1 2023-08-04T20:16:59.292Z aaa-bbb-test 578f2f4c-/bbb/test/bbb-front - - [stream-processor@48577 log-group=\"/example/logGroupName/ThatExists\" log-stream=\"task/bbb-front-service/a4b046968c23af470b6cf9db016d4583\" account=\"1234567890\"] Example ThisBeTruncated";
        final String modifiedRecord = "<14>1 2023-08-04T20:16:59.292Z 1234567890.host.example.com exampleAppName - - [stream-processor@48577 log-group=\"/example/logGroupName/ThatExists\" log-stream=\"task/bbb-front-service/a4b046968c23af470b6cf9db016d4583\" account=\"1234567890\"] Example";
        sendRecord(record.getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals(modifiedRecord, new String(spoolList.get(0), StandardCharsets.UTF_8));
    }

    @Test
    public void sendCfe16SpoolTruncatedTest() {
        final String record = "<14>1 2023-08-07T08:39:43.196Z CFE-16 capsulated - - [CFE-16-metadata@48577 authentication_token=\"My RoutingKey having token\" channel=\"defaultchannel\" time_source=\"generated\"][CFE-16-origin@48577 X-Forwarded-For=\"127.0.0.3\" X-Forwarded-Host=\"127.0.0.2\" X-Forwarded-Proto=\"http\"][event_id@48577 hostname=\"relay.example.com\" uuid=\"029EF30A9CB94D32BE40D3DCD01765AA\" unixtime=\"1691408383\" id_source=\"relay\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"relay.example.com\" source=\"localhost\" source_module=\"imptcp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"relay.example.com\" version_source=\"relay\"] \"Testing\" ThisBeTruncated";
        final String modifiedRecord = "<14>1 2023-08-07T08:39:43.196Z my-routingkey-having-hostname.example.com capsulated - - [CFE-16-metadata@48577 authentication_token=\"My RoutingKey having token\" channel=\"defaultchannel\" time_source=\"generated\"][CFE-16-origin@48577 X-Forwarded-For=\"127.0.0.3\" X-Forwarded-Host=\"127.0.0.2\" X-Forwarded-Proto=\"http\"][event_id@48577 hostname=\"relay.example.com\" uuid=\"029EF30A9CB94D32BE40D3DCD01765AA\" unixtime=\"1691408383\" id_source=\"relay\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"relay.example.com\" source=\"localhost\" source_module=\"imptcp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"relay.example.com\" version_source=\"relay\"] \"Testing\"";
        sendRecord(record.getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals(modifiedRecord, new String(spoolList.get(0), StandardCharsets.UTF_8));
    }
}
