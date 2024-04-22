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

import com.teragrep.cfe_35.config.RoutingConfig;
import com.teragrep.rlp_01.RelpBatch;
import com.teragrep.rlp_01.RelpConnection;

import com.teragrep.rlp_03.channel.socket.PlainFactory;
import com.teragrep.rlp_03.eventloop.EventLoop;
import com.teragrep.rlp_03.eventloop.EventLoopFactory;
import com.teragrep.rlp_03.frame.delegate.DefaultFrameDelegate;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.server.ServerFactory;
import org.junit.jupiter.api.*;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OutputFailureTest {

    final int port = 7600;

    private void setupRouter() {
        System.setProperty("cfe07LookupPath", "src/test/resources/cfe_07");
        System.setProperty("cfe16LookupPath", "src/test/resources/cfe_16");
        System.setProperty("kin02LookupPath", "src/test/resources/kin_02");
        System.setProperty("routingTargetsConfig", "src/test/resources/targetsOutputFailureTest.json");
        System.setProperty("listenPort", String.valueOf(port));

        Thread routerServer = new Thread(() -> {
            RoutingConfig routingConfig;
            try {
                routingConfig = new RoutingConfig();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println("Starting the router");
            try (Router router = new Router(routingConfig)) {
                Thread.sleep(Long.MAX_VALUE);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        routerServer.start();
    }

    private final List<byte[]> spoolList = new ArrayList<>();
    private EventLoop spoolEventLoop;
    private final List<byte[]> inspectionList = new ArrayList<>();
    private EventLoop inspectionEventLoop;

    public void setupTargets() throws IOException {
        spoolEventLoop = setup(7601, spoolList);
        inspectionEventLoop = setup(7602, inspectionList);
    }

    private EventLoop setup(int port, List<byte[]> recordList) throws IOException {
        Consumer<FrameContext> cbFunction = relpFrameServerRX -> recordList
                .add(relpFrameServerRX.relpFrame().payload().toBytes());

        EventLoopFactory eventLoopFactory = new EventLoopFactory();
        EventLoop eventLoop = eventLoopFactory.create(); // FIXME this is not cleaned up
        Thread eventLoopThread = new Thread(eventLoop);
        eventLoopThread.start(); // FIXME this is not cleaned up

        ExecutorService executorService = Executors.newSingleThreadExecutor(); // FIXME this is not cleaned up

        ServerFactory serverFactory = new ServerFactory(
                eventLoop,
                executorService,
                new PlainFactory(),
                () -> new DefaultFrameDelegate(cbFunction)
        );
        serverFactory.create(port);

        return eventLoop;
    }

    private void teardownTargets() throws InterruptedException {
        spoolEventLoop.stop();
        inspectionEventLoop.stop();
    }

    @Test
    public void testSend() throws IOException, InterruptedException, TimeoutException {
        System.setProperty("reconnectInterval", "1000");
        String spoolMessage = "<14>1 2020-05-15T13:24:03.603Z performance-test-host performance-test-tag - - - hello";
        String expected = "Expected timeout happening";
        setupRouter();
        setupTargets();
        Thread.sleep(1000);
        RelpConnection relpConnection = new RelpConnection();
        relpConnection.connect("127.0.0.1", port);
        RelpBatch batch = new RelpBatch();
        batch.insert(spoolMessage.getBytes(StandardCharsets.UTF_8));
        teardownTargets();
        try {
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> relpConnection.commit(batch), expected);
            Assertions.fail("Timeout didn't proc");
        }
        catch (AssertionFailedError e) {
            if (!e.toString().contains(expected)) {
                Assertions.fail("Did not timeout as expected: " + e);
            }
        }
        setupTargets();
        // Just to let the server come online
        Thread.sleep(5000);
        Assertions.assertEquals(spoolMessage, new String(spoolList.get(0), StandardCharsets.UTF_8));
    }

    @Test
    public void testNonExistentConnect() throws IOException, InterruptedException, TimeoutException {
        System.setProperty("reconnectInterval", "1000");
        RelpConnection relpConnection = new RelpConnection();
        Assertions.assertThrows(IOException.class, () -> relpConnection.connect("127.0.0.1", 1234));
    }
}
