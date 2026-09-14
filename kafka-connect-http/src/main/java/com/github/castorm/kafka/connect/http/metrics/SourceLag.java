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

import lombok.extern.slf4j.Slf4j;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Reports how far behind its source an HTTP source task is running, as
 * {@code now - the timestamp of the last committed offset}.
 *
 * ENG-2661. Debezium-backed sources expose MilliSecondsBehindSource and the source dashboards and
 * lag alerts are built on it; HTTP sources had no equivalent, so a connector could run days behind
 * while reporting healthy. The value is trivially derivable here because the committed offset
 * already carries a source timestamp.
 *
 * Registered under a Streamkap-owned JMX domain, but deliberately shaped like Debezium's
 * connector-metrics MBean (same attribute name, same context/server key properties) so the JMX
 * exporter can publish it under the metric name the existing dashboards and alerts already query.
 *
 * One MBean per endpoint. Each endpoint is owned by exactly one task, so the names stay unique
 * however many tasks the connector runs.
 */
@Slf4j
public class SourceLag implements SourceLagMBean {

    private static final String DOMAIN = "streamkap.http";

    private static final long UNKNOWN = -1L;

    private final String connectorName;

    private final String endpoint;

    private final Supplier<Optional<Instant>> offsetTimestamp;

    private volatile boolean caughtUp = false;

    private ObjectName registeredName;

    public SourceLag(String connectorName, String endpoint, Supplier<Optional<Instant>> offsetTimestamp) {
        this.connectorName = sanitize(connectorName);
        this.endpoint = sanitize(endpoint);
        this.offsetTimestamp = offsetTimestamp;
    }

    @Override
    public long getMilliSecondsBehindSource() {
        // Read through to the live offset on every call - it is replaced on each commit, so the
        // value is always current without needing anything to push updates.
        Optional<Instant> timestamp = offsetTimestamp.get();
        if (!timestamp.isPresent()) {
            return UNKNOWN;
        }
        // A drained endpoint is not behind. Without this, a source that is quiet - hourly batches,
        // business hours only - reports an ever-growing `now - cursor` and trips the lag alert while
        // perfectly healthy. A polling source can tell idle from stuck (the response came back with
        // nothing), which is why this needs no synthetic traffic the way a log reader does.
        if (caughtUp) {
            return 0L;
        }
        return Math.max(0L, System.currentTimeMillis() - timestamp.get().toEpochMilli());
    }

    /**
     * Records whether the last response drained the endpoint. Set from the total the API returned,
     * not from what survived the offset filter: a full page of already-seen records means the cursor
     * has not yet caught up with what was read, which is still behind.
     */
    public void setCaughtUp(boolean caughtUp) {
        this.caughtUp = caughtUp;
    }

    /**
     * Registers the MBean. Never throws - failing to publish a metric must not stop a task running.
     */
    public void register() {
        try {
            ObjectName name = objectName();
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (server.isRegistered(name)) {
                // A previous task instance for this endpoint did not unregister cleanly.
                server.unregisterMBean(name);
            }
            server.registerMBean(this, name);
            registeredName = name;
            log.info("Registered source lag metric {}", name);
        } catch (Exception e) {
            log.warn("Could not register source lag metric for connector {} endpoint {}. "
                    + "The task will run normally but will not report lag.", connectorName, endpoint, e);
        }
    }

    /**
     * Unregisters the MBean. Never throws.
     */
    public void unregister() {
        if (registeredName == null) {
            return;
        }
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(registeredName);
        } catch (Exception e) {
            log.warn("Could not unregister source lag metric {}", registeredName, e);
        } finally {
            registeredName = null;
        }
    }

    ObjectName objectName() throws MalformedObjectNameException {
        return new ObjectName(String.format(
                "%s:type=connector-metrics,context=streaming,server=%s,endpoint=%s",
                DOMAIN, connectorName, endpoint));
    }

    /**
     * Strips characters that are not legal in an ObjectName value. Elasticsearch forbids most of
     * these in index names and connector names are generated, so this should never fire - it is
     * here so a surprising name degrades the metric rather than the task.
     */
    private static String sanitize(String value) {
        return value == null || value.isEmpty() ? "unknown" : value.replaceAll("[:,=*?\"\\s]", "_");
    }
}
