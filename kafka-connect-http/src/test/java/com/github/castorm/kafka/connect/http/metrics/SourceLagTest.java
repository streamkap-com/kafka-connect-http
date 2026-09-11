package com.github.castorm.kafka.connect.http.metrics;

/*-
 * #%L
 * Kafka Connect HTTP
 * %%
 * Copyright (C) 2020 - 2024 Cástor Rodríguez
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class SourceLagTest {

    @Test
    void whenOffsetTimestampIsOld_thenLagIsTheDifference() {
        Instant tenMinutesAgo = Instant.now().minusSeconds(600);
        SourceLag lag = new SourceLag("source_abc", "orders", () -> Optional.of(tenMinutesAgo));

        assertThat(lag.getMilliSecondsBehindSource()).isBetween(600_000L, 610_000L);
    }

    @Test
    void whenNoOffsetTimestamp_thenUnknown() {
        SourceLag lag = new SourceLag("source_abc", "orders", Optional::empty);

        assertThat(lag.getMilliSecondsBehindSource()).isEqualTo(-1L);
    }

    @Test
    void whenOffsetTimestampIsInTheFuture_thenNeverNegative() {
        Instant later = Instant.now().plusSeconds(60);
        SourceLag lag = new SourceLag("source_abc", "orders", () -> Optional.of(later));

        assertThat(lag.getMilliSecondsBehindSource()).isZero();
    }

    @Test
    void whenOffsetAdvances_thenValueFollowsIt() {
        AtomicReference<Instant> current = new AtomicReference<>(Instant.now().minusSeconds(3600));
        SourceLag lag = new SourceLag("source_abc", "orders", () -> Optional.of(current.get()));

        long before = lag.getMilliSecondsBehindSource();
        current.set(Instant.now());

        assertThat(before).isGreaterThan(lag.getMilliSecondsBehindSource());
    }

    @Test
    void whenNamed_thenObjectNameIsShapedLikeTheDebeziumOne() throws Exception {
        SourceLag lag = new SourceLag("source_abc", "orders", Optional::empty);

        assertThat(lag.objectName().toString()).isEqualTo(
                "streamkap.http:type=connector-metrics,context=streaming,server=source_abc,endpoint=orders");
    }

    @Test
    void whenNameHasIllegalCharacters_thenTheyAreReplaced() throws Exception {
        SourceLag lag = new SourceLag("source_abc", "bad=name,with:chars", Optional::empty);

        assertThat(lag.objectName().getKeyProperty("endpoint")).isEqualTo("bad_name_with_chars");
    }

    @Test
    void whenNameIsMissing_thenPlaceholderUsed() throws Exception {
        SourceLag lag = new SourceLag(null, "orders", Optional::empty);

        assertThat(lag.objectName().getKeyProperty("server")).isEqualTo("unknown");
    }

    @Test
    void whenRegistered_thenReadableFromTheMBeanServerAndRemovedOnStop() throws Exception {
        Instant fiveMinutesAgo = Instant.now().minusSeconds(300);
        SourceLag lag = new SourceLag("source_reg", "orders", () -> Optional.of(fiveMinutesAgo));

        lag.register();
        Object value = ManagementFactory.getPlatformMBeanServer()
                .getAttribute(lag.objectName(), "MilliSecondsBehindSource");
        assertThat((Long) value).isBetween(300_000L, 310_000L);

        lag.unregister();
        assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(lag.objectName())).isFalse();
    }

    @Test
    void whenRegisteredTwice_thenSecondReplacesFirstRatherThanFailing() {
        SourceLag first = new SourceLag("source_dup", "orders", Optional::empty);
        SourceLag second = new SourceLag("source_dup", "orders", Optional::empty);

        first.register();
        second.register();

        try {
            assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(second.objectName())).isTrue();
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            second.unregister();
        }
    }

    @Test
    void whenUnregisterCalledWithoutRegister_thenNoError() {
        new SourceLag("source_abc", "orders", Optional::empty).unregister();
    }
}
