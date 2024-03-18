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

package software.amazon.kinesis.connectors.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import software.amazon.kinesis.connectors.flink.migrator.FlinkKinesisConsumerMigratorFunctionInitializationContext;

import java.util.List;
import java.util.Properties;

public class FlinkKinesisConsumerMigrator<T> extends FlinkKinesisConsumer<T> {

    public FlinkKinesisConsumerMigrator(final String stream, final DeserializationSchema<T> deserializer, final Properties configProps) {
        super(stream, deserializer, configProps);
    }

    public FlinkKinesisConsumerMigrator(final String stream, final KinesisDeserializationSchema<T> deserializer, final Properties configProps) {
        super(stream, deserializer, configProps);
    }

    public FlinkKinesisConsumerMigrator(final List<String> streams, final KinesisDeserializationSchema<T> deserializer, final Properties configProps) {
        super(streams, deserializer, configProps);
    }

    @Override
    public void initializeState(final FunctionInitializationContext context) throws Exception {
        super.initializeState(new FlinkKinesisConsumerMigratorFunctionInitializationContext(context, getRuntimeContext().getExecutionConfig()));
    }
}
