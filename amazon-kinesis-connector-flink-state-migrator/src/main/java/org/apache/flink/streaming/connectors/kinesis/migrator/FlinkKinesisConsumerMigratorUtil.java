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

public class FlinkKinesisConsumerMigratorUtil {
    public static final String STATE_NAME = "Kinesis-Stream-Shard-State";

    private FlinkKinesisConsumerMigratorUtil() {
        // prevent instantiation
    }
    public static Tuple2<
            StreamShardMetadata,
            SequenceNumber> mapAmazonStateToApache(
            final Tuple2<StreamShardMetadata, SequenceNumber> amazonState) {

        final StreamShardMetadata apacheStreamShardMetadata =
                new StreamShardMetadata();

        final StreamShardMetadata amazonStreamShardMetadata = amazonState.getField(0);
        apacheStreamShardMetadata.setShardId(amazonStreamShardMetadata.getShardId());
        apacheStreamShardMetadata.setParentShardId(amazonStreamShardMetadata.getParentShardId());
        apacheStreamShardMetadata.setAdjacentParentShardId(amazonStreamShardMetadata.getAdjacentParentShardId());
        apacheStreamShardMetadata.setStreamName(amazonStreamShardMetadata.getStreamName());
        apacheStreamShardMetadata.setEndingHashKey(amazonStreamShardMetadata.getEndingHashKey());
        apacheStreamShardMetadata.setStartingHashKey(amazonStreamShardMetadata.getStartingHashKey());
        apacheStreamShardMetadata.setEndingSequenceNumber(amazonStreamShardMetadata.getEndingSequenceNumber());
        apacheStreamShardMetadata.setStartingSequenceNumber(amazonStreamShardMetadata.getStartingSequenceNumber());

        final SequenceNumber amazonSequenceNumber = amazonState.getField(1);
        final SequenceNumber apacheSequenceNumber =
                new SequenceNumber(
                        amazonSequenceNumber.getSequenceNumber(),
                        amazonSequenceNumber.getSubSequenceNumber());

        return Tuple2.of(apacheStreamShardMetadata, apacheSequenceNumber);
    }
}
