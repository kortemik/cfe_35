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
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.server.ServerFactory;
import com.teragrep.rlp_03.frame.delegate.DefaultFrameDelegate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfSystemProperty(
        named = "runPerformanceTest",
        matches = "true"
)
public class PerformanceTest {

    private final AtomicInteger spoolCount = new AtomicInteger(0);
    private final AtomicInteger inspectionCount = new AtomicInteger(0);
    private final AtomicInteger siem0Count = new AtomicInteger(0);
    private final AtomicInteger hdfsCount = new AtomicInteger(0);
    private final AtomicInteger deadLetterCount = new AtomicInteger(0);

    @BeforeAll
    public void setupTargets() throws Exception {
        setup(4601, spoolCount);
        setup(4602, inspectionCount);
        setup(4603, siem0Count);
        setup(4604, hdfsCount);
        setup(4605, deadLetterCount);
        System.setProperty("cfe07LookupPath", "src/test/resources/cfe_07");
        System.setProperty("cfe16LookupPath", "src/test/resources/cfe_16");
        System.setProperty("kin02LookupPath", "src/test/resources/kin_02");
        System.setProperty("routingTargetsConfig", "src/test/resources/targetsPerformanceTest.json");
        System.setProperty("listenPort", "4600");

        Thread server = new Thread(() -> {
            RoutingConfig routingConfig = null;
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
                System.out.println("Server failed: " + e);
            }
        });
        server.start();
    }

    private void setup(int port, AtomicInteger count) throws IOException {
        Consumer<FrameContext> cbFunction = (message) -> {
            count.getAndIncrement();
        };

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
    }

    @Test
    public void runCount() throws IOException, TimeoutException, InterruptedException {
        Thread.sleep(1000);
        System.out.println("runCount");
        RelpConnection relpConnection = new RelpConnection();
        relpConnection.connect("127.0.0.1", 4600);
        byte[] spoolMessage = "<14>1 2020-05-15T13:24:03.603Z performance-test-host performance-test-tag - - - hello"
                .getBytes("UTF-8");
        byte[] deadletterMessage = "<14>1 2020-05-15T13:24:03.603Z not-good-host not-good-tag - - - hello so this is same"
                .getBytes("UTF-8");
        byte[] inspectionMessage = "Come get your hotdogs, hotdogs for everyone!".getBytes("UTF-8");
        Instant start = Instant.now();
        int rounds = 10;
        int eventsPerBatch = 10000;
        for (int round = 1; round <= rounds; round++) {
            RelpBatch batch = new RelpBatch();
            for (int i = 0; i < eventsPerBatch; i++) {
                batch.insert(spoolMessage);
                batch.insert(deadletterMessage);
                // batch.insert(inspectionMessage); // It gets VERBOSE.
            }
            relpConnection.commit(batch);
        }
        relpConnection.disconnect();
        Instant end = Instant.now();
        float elapsed = (float) (end.toEpochMilli() - start.toEpochMilli()) / 1000;
        int totalMessages = spoolCount.get() + inspectionCount.get() + siem0Count.get() + hdfsCount.get()
                + deadLetterCount.get();
        System.out
                .println(
                        "Sent " + totalMessages + " messages in " + elapsed + "s, (" + totalMessages / elapsed + " EPS)"
                );
        System.out.println("Messages received:");
        System.out.println("spoolCount: " + spoolCount);
        System.out.println("inspectionCount: " + inspectionCount);
        System.out.println("siem0Count: " + siem0Count);
        System.out.println("hdfsCount: " + hdfsCount);
        System.out.println("deadLetterCount: " + deadLetterCount);
    }
}
