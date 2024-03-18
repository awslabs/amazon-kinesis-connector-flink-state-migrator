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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import java.util.OptionalLong;

public class FlinkKinesisConsumerMigratorFunctionInitializationContext implements FunctionInitializationContext {

    private final FunctionInitializationContext delegate;
    private final OperatorStateStore operatorStateStore;

    public FlinkKinesisConsumerMigratorFunctionInitializationContext(
            final FunctionInitializationContext delegate,
            final ExecutionConfig executionConfig) {
        this.delegate = delegate;
        this.operatorStateStore = new FlinkKinesisConsumerMigratorOperatorStateStore(
                delegate.getOperatorStateStore(), executionConfig);
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return delegate.getRestoredCheckpointId();
    }

    @Override
    public OperatorStateStore getOperatorStateStore() {
        return operatorStateStore;
    }

    @Override
    public KeyedStateStore getKeyedStateStore() {
        return delegate.getKeyedStateStore();
    }

    @Override
    public boolean isRestored() {
        return delegate.isRestored();
    }
}
