package com.github.castorm.kafka.connect.http;

/*-
 * #%L
 * kafka-connect-http
 * %%
 * Copyright (C) 2020 CastorM
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

import com.github.castorm.kafka.connect.http.ack.ConfirmationWindow;
import com.github.castorm.kafka.connect.http.client.spi.HttpClient;
import com.github.castorm.kafka.connect.http.model.HttpRequest;
import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.Offset;
import com.github.castorm.kafka.connect.http.metrics.SourceLag;
import com.github.castorm.kafka.connect.http.model.Partition;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordFilterFactory;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordSorter;
import com.github.castorm.kafka.connect.http.request.spi.HttpRequestFactory;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import com.github.castorm.kafka.connect.timer.TimerThrottler;
import edu.emory.mathcs.backport.java.util.Collections;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.github.castorm.kafka.connect.common.VersionUtils.getVersion;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

@Slf4j
public class HttpSourceTaskSingleEndpoint extends SourceTask {

    private final Function<Map<String, String>, HttpSourceConnectorConfig> configFactory;

    private TimerThrottler throttler;

    private HttpRequestFactory requestFactory;

    private HttpClient requestExecutor;

    private HttpResponseParser responseParser;

    private SourceRecordSorter recordSorter;

    private SourceRecordFilterFactory recordFilterFactory;

    private ConfirmationWindow<Map<String, ?>> confirmationWindow = new ConfirmationWindow<>(emptyList());

    @Getter
    // Written by the task thread in start() and by the offset-committer thread in commit(), read by
    // both of those and by the JMX thread behind the lag metric. Volatile gives those reads a
    // happens-before edge on the last write; a single writer at a time means nothing stronger is
    // needed. Not related to the offsets Connect persists - those travel on the records poll()
    // returns - this is only the cursor used to build the next request.
    private volatile Offset offset;

    @Setter
    @Getter
    private String endpoint;

    private SourceLag sourceLag;

    HttpSourceTaskSingleEndpoint(String endpoint, Function<Map<String, String>, HttpSourceConnectorConfig> configFactory) {
        this.configFactory = configFactory;
        this.endpoint = endpoint;
    }

    public HttpSourceTaskSingleEndpoint(String endpoint) {
        this(endpoint, HttpSourceConnectorConfig::new);
    }

    @Override
    public void start(Map<String, String> settings) {

        HttpSourceConnectorConfig config = configFactory.apply(settings);

        throttler = config.getThrottler();
        requestFactory = config.getRequestFactory();
        requestExecutor = config.getClient();
        responseParser = config.getResponseParser();
        recordSorter = config.getRecordSorter();
        recordFilterFactory = config.getRecordFilterFactory();
        offset = loadOffset(this.context, config.getInitialOffset());

        // ENG-2661. Reads the offset field on each scrape rather than being pushed updates.
        // The field is volatile, which is what makes a scrape see the last committed value.
        sourceLag = new SourceLag(settings.get("name"), endpoint, () -> offset.getTimestamp());
        sourceLag.register();
    }

    private Offset loadOffset(SourceTaskContext context, Map<String, String> initialOffset) {
        Map<String, Object> restoredOffset = ofNullable(
            context.offsetStorageReader().offset(
                Partition.getPartition(endpoint))).orElseGet(Collections::emptyMap);
        return Offset.of(!restoredOffset.isEmpty() ? restoredOffset : initialOffset);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        throttler.throttle(offset.getTimestamp().orElseGet(Instant::now));

        HttpRequest request = requestFactory.createRequest(offset);

        HttpResponse response = execute(request);

        List<SourceRecord> records = responseParser.parse(endpoint, response);

        sourceLag.pollCompleted(records.isEmpty());

        List<SourceRecord> unseenRecords = recordSorter.sort(records).stream()
                .filter(recordFilterFactory.create(offset))
                .collect(toList());

        log.info("Request for offset {} yields {}/{} new records, endpoint {}", offset.toMap(), unseenRecords.size(), records.size(), endpoint);

        confirmationWindow = new ConfirmationWindow<>(extractOffsets(unseenRecords));

        return unseenRecords;
    }

    private HttpResponse execute(HttpRequest request) {
        try {
            return requestExecutor.execute(request);
        } catch (IOException e) {
            sourceLag.pollFailed();
            throw new RetriableException(e);
        }
    }

    private static List<Map<String, ?>> extractOffsets(List<SourceRecord> recordsToSend) {
        return recordsToSend.stream()
                .map(SourceRecord::sourceOffset)
                .collect(toList());
    }

    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        confirmationWindow.confirm(record.sourceOffset());
    }

    public void commit() {
        offset = confirmationWindow.getLowWatermarkOffset()
                .map(Offset::of)
                .orElse(offset);

        log.debug("Offset set to {}", offset);
    }

    @Override
    public void stop() {
        if (sourceLag != null) {
            sourceLag.unregister();
        }
    }

    public String version() {
        return getVersion();
    }
}
