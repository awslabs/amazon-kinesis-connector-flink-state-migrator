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

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.PartitionableListState;
import org.apache.flink.runtime.state.RegisteredOperatorStateBackendMetaInfo;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.state.OperatorStateHandle.Mode.UNION;

@Slf4j
@Builder
public class FlinkKinesisConsumerMigratorOperatorStateStore implements OperatorStateStore {
    private final OperatorStateStore delegate;
    private final ExecutionConfig executionConfig;

    @Override
    public <K, V> BroadcastState<K, V> getBroadcastState(final MapStateDescriptor<K, V> mapStateDescriptor) throws Exception {
        return delegate.getBroadcastState(mapStateDescriptor);
    }

    @Override
    public <S> ListState<S> getListState(final ListStateDescriptor<S> listStateDescriptor) throws Exception {
        return delegate.getListState(listStateDescriptor);
    }

    @Override
    public <S> ListState<S> getUnionListState(final ListStateDescriptor<S> apacheStateDescriptor) throws Exception {
        if (!FlinkKinesisConsumerMigratorUtil.STATE_NAME.equals(apacheStateDescriptor.getName())) {
            return delegate.getUnionListState(apacheStateDescriptor);
        }

        if (!delegate.getRegisteredStateNames().contains(FlinkKinesisConsumerMigratorUtil.STATE_NAME)) {
            log.info("No state found for Kinesis connector to restore");
            return delegate.getUnionListState(apacheStateDescriptor);
        }

        if (!getClassOfStreamShardMetadata().getName().contains("amazon")) {
            log.info("Kinesis connector state was created using Apache Kinesis connector, not performing migration");
            return delegate.getUnionListState(apacheStateDescriptor);
        }

        log.info("Kinesis connector state was created using Amazon Kinesis connector. Migrating to Apache connector");
        return migrateAmazonStateToApache(apacheStateDescriptor);
    }

    @Override
    public Set<String> getRegisteredStateNames() {
        return delegate.getRegisteredStateNames();
    }

    @Override
    public Set<String> getRegisteredBroadcastStateNames() {
        return delegate.getRegisteredBroadcastStateNames();
    }

    public <S> ListState<S> migrateAmazonStateToApache(final ListStateDescriptor<S> apacheStateDescriptor) throws Exception {
        final ListStateDescriptor<Tuple2<software.amazon.kinesis.connectors.flink.model.StreamShardMetadata, software.amazon.kinesis.connectors.flink.model.SequenceNumber>> amazonListStateDescriptor = new ListStateDescriptor<>(FlinkKinesisConsumerMigratorUtil.STATE_NAME,
                new TupleTypeInfo<>(TypeInformation.of(software.amazon.kinesis.connectors.flink.model.StreamShardMetadata.class), TypeInformation.of(software.amazon.kinesis.connectors.flink.model.SequenceNumber.class)));

        final ListState<S> amazonListState = (ListState<S>) delegate.getUnionListState(amazonListStateDescriptor);

        Preconditions.checkState(amazonListState instanceof PartitionableListState,
                "Expecting amazonListState to be PartitionableListState but was: " + amazonListState.getClass().getName());

        apacheStateDescriptor.initializeSerializerUnlessSet(executionConfig);

        PartitionableListState<S> partitionableListState = (PartitionableListState<S>) amazonListState;
        partitionableListState.setStateMetaInfo(new RegisteredOperatorStateBackendMetaInfo<>(FlinkKinesisConsumerMigratorUtil.STATE_NAME, apacheStateDescriptor.getElementSerializer(), UNION));

        final List<Tuple2<org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata,
                org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber>> mappedState =
                StreamSupport.stream(amazonListState.get().spliterator(), false)
                        .map(e -> FlinkKinesisConsumerMigratorUtil.mapAmazonStateToApache(
                                (Tuple2<
                                        software.amazon.kinesis.connectors.flink.model.StreamShardMetadata,
                                        software.amazon.kinesis.connectors.flink.model.SequenceNumber>) e))
                        .collect(Collectors.toList());

        amazonListState.clear();

        mappedState.forEach(s -> {
            try {
                amazonListState.add((S) s);
            } catch (Exception e) {
                throw new RuntimeException("Failed to migrate Amazon Kinesis connector state to Apache model", e);
            }
        });

        return amazonListState;
    }

    private Class<?> getClassOfStreamShardMetadata() throws Exception {
        final Field field = delegate.getClass().getDeclaredField("registeredOperatorStates");
        field.setAccessible(true);

        final Map<String, PartitionableListState<?>> registeredState = (Map<String, PartitionableListState<?>>) field.get(delegate);
        final TupleSerializer registeredSerializer = (TupleSerializer) registeredState.get(FlinkKinesisConsumerMigratorUtil.STATE_NAME).getStateMetaInfo().getPartitionStateSerializer();
        final PojoSerializer pojoSerializer = (PojoSerializer) registeredSerializer.getFieldSerializers()[0];

        final Method method = pojoSerializer.getClass().getDeclaredMethod("getPojoClass");
        method.setAccessible(true);

        return (Class<?>) method.invoke(pojoSerializer);
    }
}
