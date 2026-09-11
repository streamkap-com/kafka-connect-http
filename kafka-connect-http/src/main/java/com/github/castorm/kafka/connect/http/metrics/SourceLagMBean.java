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

/**
 * Standard MBean interface. The name must stay {@code <implementation>MBean} for JMX to find it.
 */
public interface SourceLagMBean {

    /**
     * Milliseconds between now and the timestamp of the last committed offset, or -1 when the
     * task has no offset timestamp yet.
     */
    long getMilliSecondsBehindSource();
}
