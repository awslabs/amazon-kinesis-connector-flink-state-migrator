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

package software.amazon.kinesis.connectors.flink.migrator;

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.kinesis.connectors.flink.migrator.FlinkKinesisConsumerMigratorFunctionInitializationContext;
import software.amazon.kinesis.connectors.flink.migrator.FlinkKinesisConsumerMigratorOperatorStateStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class FlinkKinesisConsumerMigratorFunctionInitializationContextTest {

    @InjectMocks
    private FlinkKinesisConsumerMigratorFunctionInitializationContext context;

    @Mock
    private FunctionInitializationContext delegate;

    @Mock
    private OperatorStateStore operatorStateStore;

    @Test
    void getRestoredCheckpointId() {
        context.getRestoredCheckpointId();

        verify(delegate).getRestoredCheckpointId();
    }

    @Test
    void getKeyedStateStore() {
        context.getKeyedStateStore();

        verify(delegate).getKeyedStateStore();
    }

    @Test
    void isRestored() {
        context.isRestored();

        verify(delegate).isRestored();
    }

    @Test
    void getOperatorStateStore() {
        OperatorStateStore actual = context.getOperatorStateStore();

        assertThat(actual).isInstanceOf(FlinkKinesisConsumerMigratorOperatorStateStore.class);
    }
}
