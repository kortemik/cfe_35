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
package com.teragrep.cfe_35.output;

import com.teragrep.rlp_03.client.Client;
import com.teragrep.rlp_03.client.ClientFactory;
import com.teragrep.rlp_03.client.ClientStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ClientPool implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientPool.class);

    private final ClientFactory clientFactory;
    private final InetSocketAddress inetAddress;
    private final long connectionTimeout;
    private final int maximumPoolSize;
    private final BlockingQueue<CompletableFuture<Client>> clients;

    private final AtomicLong currentPoolSize;

    private final AtomicBoolean close;

    private final ClientStub clientStub;

    public ClientPool(
            ClientFactory clientFactory,
            InetSocketAddress inetSocketAddress,
            long connectionTimeout,
            int maximumPoolSize
    ) {
        this(
                clientFactory,
                inetSocketAddress,
                connectionTimeout,
                maximumPoolSize,
                new ArrayBlockingQueue<>(maximumPoolSize),
                new AtomicLong(),
                new AtomicBoolean(),
                new ClientStub()
        );
    }

    public ClientPool(
            ClientFactory clientFactory,
            InetSocketAddress inetSocketAddress,
            long connectionTimeout,
            int maximumPoolSize,
            BlockingQueue<CompletableFuture<Client>> clients,
            AtomicLong currentPoolSize,
            AtomicBoolean close,
            ClientStub clientStub
    ) {
        this.clientFactory = clientFactory;
        this.inetAddress = inetSocketAddress;
        this.connectionTimeout = connectionTimeout;
        this.maximumPoolSize = maximumPoolSize;
        this.clients = clients;
        this.currentPoolSize = currentPoolSize;
        this.close = close;
        this.clientStub = clientStub;
    }

    CompletableFuture<Client> take() throws InterruptedException {
        CompletableFuture<Client> client = new CompletableFuture<>();
        if (close.get()) {
            client.complete(clientStub);
        }
        else {
            long poolSize = currentPoolSize.get();
            if (poolSize < maximumPoolSize) {
                if (currentPoolSize.compareAndSet(poolSize, poolSize + 1)) {
                    CompletableFuture<Client> newClient = clientFactory.open(inetAddress);
                    clients.put(newClient);
                }
            }
            client = clients.take();
        }
        return client;
    }

    public void offer(Client client) throws InterruptedException {
        if (close.get()) {
            client.close();
        }
        else {
            CompletableFuture<Client> clientCompletableFuture = new CompletableFuture<>();
            clientCompletableFuture.complete(client);
            clients.put(clientCompletableFuture);
        }
    }

    @Override
    public void close() {
        close.set(true);

        CompletableFuture<Client> clientCompletableFuture = clients.poll();
        while (clientCompletableFuture != null) {

            clientCompletableFuture.whenComplete((client, throwable) -> {
                if (client != null) {
                    client.close();
                }
                else if (throwable != null) {
                    LOGGER.warn("Exception while closing a Client", throwable);
                }
            });
            clientCompletableFuture = clients.poll();
        }
    }
}
