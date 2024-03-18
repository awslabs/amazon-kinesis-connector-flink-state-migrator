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

import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import software.amazon.kinesis.connectors.flink.FlinkKinesisConsumerMigrator;
import software.amazon.kinesis.connectors.flink.model.SequenceNumber;
import software.amazon.kinesis.connectors.flink.model.StreamShardMetadata;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FlinkKinesisConsumerMigratorTest {

    @Test
    void nothingToRestore() throws Exception {
        FlinkKinesisConsumerMigrator<String> stateMigratingSource = getStateMigratingSource();

        stateMigratingSource.initializeState(mockState(new TestListState<>()));

        assertThat(getState(stateMigratingSource)).isEmpty();
    }

    private FlinkKinesisConsumerMigrator<String> getStateMigratingSource() {
        Properties properties = new Properties();
        properties.setProperty(AWS_REGION, "us-east-2");

        FlinkKinesisConsumerMigrator<String> stateMigratingSource = new FlinkKinesisConsumerMigrator<>("stream", new SimpleStringSchema(), properties);
        stateMigratingSource.setRuntimeContext(new MockStreamingRuntimeContext(true, 2, 0));
        return stateMigratingSource;
    }

    @SneakyThrows
    private HashMap<StreamShardMetadata.EquivalenceWrapper, SequenceNumber> getState(
            final FlinkKinesisConsumerMigrator<String> stateMigratingSource) {
        Field field = FlinkKinesisConsumer.class.getDeclaredField("sequenceNumsToRestore");
        field.setAccessible(true);
        return (HashMap<StreamShardMetadata.EquivalenceWrapper, SequenceNumber>) field.get(stateMigratingSource);
    }

    @SneakyThrows
    private <T> StateInitializationContext mockState(TestListState<T> state) {
        OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
        when(operatorStateStore.getUnionListState(any())).thenReturn((ListState<Object>) state);

        StateInitializationContext initializationContext = mock(StateInitializationContext.class);
        when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
        when(initializationContext.isRestored()).thenReturn(true);

        return initializationContext;
    }

    private static final class TestListState<T> implements ListState<T> {

        private final List<T> list = new ArrayList<>();

        @Override
        public void clear() {
            list.clear();
        }

        @Override
        public Iterable<T> get() throws Exception {
            return list;
        }

        @Override
        public void add(T value) throws Exception {
            list.add(value);
        }

        @Override
        public void update(List<T> values) throws Exception {
            list.clear();

            addAll(values);
        }

        @Override
        public void addAll(List<T> values) throws Exception {
            if (values != null) {
                list.addAll(values);
            }
        }
    }
}
