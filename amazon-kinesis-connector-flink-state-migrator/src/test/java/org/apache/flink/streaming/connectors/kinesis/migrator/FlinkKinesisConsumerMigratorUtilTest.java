/*
 *   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.migrator;

import org.apache.flink.api.java.tuple.Tuple2;
import software.amazon.kinesis.connectors.flink.model.SequenceNumber;
import software.amazon.kinesis.connectors.flink.model.StreamShardMetadata;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FlinkKinesisConsumerMigratorUtilTest {

    @Test
    void mapAmazonStateToApache() {
        StreamShardMetadata amazonStreamShardMetadata = getAmazonStreamShardMetadata();
        SequenceNumber amazonSequenceNumber = getAmazonSequenceNumber();

        Tuple2<StreamShardMetadata, SequenceNumber> apacheState = FlinkKinesisConsumerMigratorUtil
                .mapAmazonStateToApache(Tuple2.of(amazonStreamShardMetadata, amazonSequenceNumber));

        assertThat(apacheState.f0.getStreamName()).isEqualTo(amazonStreamShardMetadata.getStreamName());
        assertThat(apacheState.f0.getShardId()).isEqualTo(amazonStreamShardMetadata.getShardId());
        assertThat(apacheState.f0.getAdjacentParentShardId()).isEqualTo(amazonStreamShardMetadata.getAdjacentParentShardId());
        assertThat(apacheState.f0.getParentShardId()).isEqualTo(amazonStreamShardMetadata.getParentShardId());
        assertThat(apacheState.f0.getStartingHashKey()).isEqualTo(amazonStreamShardMetadata.getStartingHashKey());
        assertThat(apacheState.f0.getEndingHashKey()).isEqualTo(amazonStreamShardMetadata.getEndingHashKey());
        assertThat(apacheState.f0.getStartingSequenceNumber()).isEqualTo(amazonStreamShardMetadata.getStartingSequenceNumber());
        assertThat(apacheState.f0.getEndingSequenceNumber()).isEqualTo(amazonStreamShardMetadata.getEndingSequenceNumber());
        assertThat(apacheState.f1.getSequenceNumber()).isEqualTo(amazonSequenceNumber.getSequenceNumber());
        assertThat(apacheState.f1.getSubSequenceNumber()).isEqualTo(amazonSequenceNumber.getSubSequenceNumber());
    }

    private StreamShardMetadata getAmazonStreamShardMetadata() {
        StreamShardMetadata streamShardMetadata = new StreamShardMetadata();
        streamShardMetadata.setStreamName("stream-1");
        streamShardMetadata.setShardId("shard-00001");
        streamShardMetadata.setParentShardId("shard-00000");
        streamShardMetadata.setAdjacentParentShardId("shard-00000-a");
        streamShardMetadata.setStartingHashKey("start-hash");
        streamShardMetadata.setEndingHashKey("end-hash");
        streamShardMetadata.setStartingSequenceNumber("start-sequence");
        streamShardMetadata.setEndingSequenceNumber("end-sequence");
        return streamShardMetadata;
    }

    private SequenceNumber getAmazonSequenceNumber() {
        return new SequenceNumber("seq", 1);
    }
}
