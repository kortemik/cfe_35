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
package com.teragrep.cfe_35;

import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class FutureTest {

    @Test
    public void testMyFuture() throws ExecutionException, InterruptedException {
        CompletableFuture<String> sad = new CompletableFuture<>();
        CompletableFuture<String> das = new CompletableFuture<>();
        CompletableFuture<String> las = das
                .orTimeout(1000, TimeUnit.MILLISECONDS)
                .whenComplete(new BiConsumer<String, Throwable>() {

                    @Override
                    public void accept(String s, Throwable throwable) {
                        if (s == null && throwable != null) {
                            System.err.println("las has <" + throwable + ">");
                        }
                        else if (s != null && throwable == null) {
                            System.out.println("las has <[" + s + "]>");
                        }
                        else {
                            throw new IllegalStateException("return value and exception, this wont do!");
                        }
                    }
                });

        List<CompletableFuture<String>> lost = new LinkedList<>();

        lost.add(sad);
        lost.add(das);
        lost.add(las);

        CompletableFuture
                .allOf(lost.toArray(new CompletableFuture[0]))
                .thenRun(() -> System.out.println("new year"))
                .exceptionally(new Function<Throwable, Void>() {

                    @Override
                    public Void apply(Throwable throwable) {
                        System.err.println("you miss with <" + throwable.getMessage() + ">");
                        return null;
                    }
                });

        sad.complete("old year");
        System.out.println("so old");

        das.thenRun(() -> {
            System.out.println("bogus");
        });
        /*
        try {
            Thread.sleep(1500);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        
         */
        //das.completeExceptionally(new Throwable("not you"));
        das.complete("just good");

        System.out.println("so some");

    }

}
