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
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.teragrep.cfe_35.config.RoutingConfig;
import com.teragrep.rlp_03.eventloop.EventLoop;
import com.teragrep.rlp_03.eventloop.EventLoopFactory;
import com.teragrep.rlp_03.channel.context.ConnectContextFactory;
import com.teragrep.rlp_03.channel.socket.PlainFactory;
import com.teragrep.rlp_03.client.ClientFactory;
import com.teragrep.rlp_03.frame.delegate.DefaultFrameDelegate;
import com.teragrep.rlp_03.frame.delegate.FrameDelegate;
import com.teragrep.rlp_03.server.ServerFactory;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class Router implements AutoCloseable {

    private final RoutingLookup routingLookup;
    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final JmxReporter jmxReporter;
    private final Slf4jReporter slf4jReporter;
    private final org.eclipse.jetty.server.Server jettyServer;
    private final ExecutorService executorService;
    private final EventLoop eventLoop;
    private final Thread eventLoopThread;

    public Router(RoutingConfig routingConfig) throws IOException {
        this.jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
        this.slf4jReporter = Slf4jReporter
                .forRegistry(metricRegistry)
                .outputTo(LoggerFactory.getLogger(Router.class))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        this.routingLookup = new RoutingLookup(routingConfig);

        this.executorService = Executors.newFixedThreadPool(routingConfig.getServerThreads());
        this.eventLoop = new EventLoopFactory().create();

        this.eventLoopThread = new Thread(this.eventLoop);
        this.eventLoopThread.start();

        Supplier<FrameDelegate> routingInstanceSupplier = getFrameDelegateSupplier(routingConfig);

        ExecutorService executorService = Executors.newFixedThreadPool(routingConfig.getServerThreads());
        PlainFactory plainFactory = new PlainFactory();

        ServerFactory serverFactory = new ServerFactory(
                eventLoop,
                executorService,
                plainFactory,
                routingInstanceSupplier
        );
        serverFactory.create(routingConfig.getListenPort());

        this.jmxReporter.start();
        this.slf4jReporter.start(1, TimeUnit.MINUTES);

        // prometheus-exporter
        // https://stackoverflow.com/questions/72800851/how-to-export-metrics-collected-by-dropwizard-in-prometheus-format-from-java-a
        CollectorRegistry.defaultRegistry.register(new DropwizardExports(metricRegistry));

        jettyServer = new org.eclipse.jetty.server.Server(routingConfig.getPrometheusPort());
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        jettyServer.setHandler(context);

        MetricsServlet metricsServlet = new MetricsServlet();
        ServletHolder servletHolder = new ServletHolder(metricsServlet);
        context.addServlet(servletHolder, "/metrics");
        // Add metrics about CPU, JVM memory etc.
        DefaultExports.initialize();
        // Start the webserver.
        try {
            jettyServer.start();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private Supplier<FrameDelegate> getFrameDelegateSupplier(RoutingConfig routingConfig) {
        ConnectContextFactory connectContextFactory = new ConnectContextFactory(executorService, new PlainFactory());

        ClientFactory clientFactory = new ClientFactory(connectContextFactory, null);

        Supplier<FrameDelegate> routingInstanceSupplier = () -> {
            TargetRouting targetRouting;
            try {
                targetRouting = new ParallelTargetRouting(routingConfig, this.metricRegistry, clientFactory);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            MessageParser messageParser = new MessageParser(
                    this.routingLookup,
                    targetRouting,
                    this.metricRegistry,
                    routingConfig
            );
            return new DefaultFrameDelegate(messageParser);
        };
        return routingInstanceSupplier;
    }

    @Override
    public void close() throws Exception {
        // stop after done
        // FIXME close outputs?
        eventLoop.stop();
        executorService.shutdown();
        // TODO executorService.awaitTermination(1, TimeUnit.SECONDS); or something?
        eventLoopThread.join();
        slf4jReporter.close();
        jmxReporter.close();
        jettyServer.stop();
    }
}
