/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join.stream.state;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.IterableIterator;

import java.util.Collections;
import java.util.Iterator;

/**
 * A simple implementation of {@link MultiJoinHasUniqueKeyStateHandler} that uses a MapState to store records.
 */

// TODO This is still ongoing while still in draft PR
// We need multiple state handlers/state views:
// 1. unique key exists and is join key - use value state
// 2. unique key exists but is not join key - use keyed state
// 3. unique doesn't exist - we need to use rows as key and occurrences as count - keyed state
// coded key position here
// Add state TTL

public final class MultiJoinStateHandlers {
    
    /**
     * Interface for iterating over join records with additional metadata.
     * This allows handling both simple RowData iterators and iterators with association counts.
     */
    public interface JoinRecordIterator extends Iterator<RowData> {
        /**
         * Get the current record and its number of associations.
         * @return Tuple containing the record and its association count
         */
        Tuple2<RowData, Integer> getRecordWithAssociations();
        
        /**
         * Whether this iterator supports association counts.
         * @return true if association counts are available
         */
        boolean hasAssociationCounts();
        
        /**
         * Create a JoinRecordIterator from a regular RowData iterator.
         * @param iterator The RowData iterator
         * @return A JoinRecordIterator without association counts
         */
        static JoinRecordIterator fromRowDataIterator(Iterator<RowData> iterator) {
            return new SimpleJoinRecordIterator(iterator);
        }
        
        /**
         * Create a JoinRecordIterator from a Tuple2<RowData, Integer> iterator.
         * @param iterator The iterator with association counts
         * @return A JoinRecordIterator with association counts
         */
        static JoinRecordIterator fromTupleIterator(Iterator<Tuple2<RowData, Integer>> iterator) {
            return new AssociativeJoinRecordIterator(iterator);
        }
        
        /**
         * Create a JoinRecordIterator for a single record.
         * @param record The single record
         * @return A JoinRecordIterator with a single record
         */
        static JoinRecordIterator forSingleRecord(RowData record) {
            return fromRowDataIterator(Collections.singleton(record).iterator());
        }

        /**
         * Create a JoinRecordIterator for a single tuple record.
         * @param tuple The single tuple
         * @return A JoinRecordIterator with a single tuple record
         */
        static JoinRecordIterator forSingleRecord(Tuple2<RowData, Integer> tuple) {
            return fromTupleIterator(Collections.singleton(tuple).iterator());
        }
    }
    
    /**
     * Implementation of JoinRecordIterator for regular RowData iterators without association counts.
     */
    private static class SimpleJoinRecordIterator implements JoinRecordIterator {
        private final Iterator<RowData> iterator;
        private RowData current;
        
        SimpleJoinRecordIterator(Iterator<RowData> iterator) {
            this.iterator = iterator;
        }
        
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }
        
        @Override
        public RowData next() {
            current = iterator.next();
            return current;
        }
        
        @Override
        public Tuple2<RowData, Integer> getRecordWithAssociations() {
            return Tuple2.of(current, -1); // -1 indicates no association count available
        }
        
        @Override
        public boolean hasAssociationCounts() {
            return false;
        }
    }
    
    /**
     * Implementation of JoinRecordIterator for Tuple2<RowData, Integer> iterators with association counts.
     */
    private static class AssociativeJoinRecordIterator implements JoinRecordIterator {
        private final Iterator<Tuple2<RowData, Integer>> iterator;
        private Tuple2<RowData, Integer> current;
        
        AssociativeJoinRecordIterator(Iterator<Tuple2<RowData, Integer>> iterator) {
            this.iterator = iterator;
        }
        
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }
        
        @Override
        public RowData next() {
            current = iterator.next();
            return current.f0;
        }
        
        @Override
        public Tuple2<RowData, Integer> getRecordWithAssociations() {
            return current;
        }
        
        @Override
        public boolean hasAssociationCounts() {
            return true;
        }
    }
    
    /**
     * Common interface for join state handlers that manage state for join operations.
     */
    public interface MultiJoinStateHandler {
        /**
         * Retrieves all records stored in the state.
         *
         * @return Iterator of records
         * @throws Exception if any error occurs
         */
        Iterator<RowData> getRecords() throws Exception;
        
        /**
         * Retrieves all records with their association counts if available.
         *
         * @return JoinRecordIterator containing records and optional association counts
         * @throws Exception if any error occurs
         */
        default JoinRecordIterator getRecordsWithAssociations() throws Exception {
            return JoinRecordIterator.fromRowDataIterator(getRecords());
        }
        
        /**
         * Adds a record to the state.
         *
         * @param record The record to add
         * @throws Exception if any error occurs
         */
        void addRecord(RowData record) throws Exception;

        /**
         * Adds a record to the state.
         *
         * @param record to be deleted
         * @throws Exception if any error occurs
         */
        void retractRecord(RowData record) throws Exception;
    }
    
    public static class MultiJoinHasUniqueKeyStateHandler implements MultiJoinStateHandler {

        private final StreamingMultiJoinOperator operator;
        private final StreamOperatorStateHandler stateHandler;

        private final Configuration config;
        private final ClassLoader userCodeClassloader;
        private final JoinInputSideSpec inputSpec;
        private final long stateRetentionTime;

        private transient MapState<String, RowData> recordState;

        public MultiJoinHasUniqueKeyStateHandler(
                int inputIndex,
                StreamingMultiJoinOperator operator,
                StreamOperatorStateHandler stateHandler,
                Configuration config,
                ClassLoader userCodeClassloader,
                JoinInputSideSpec inputSpec,
                long stateRetentionTime) {
            this.operator = operator;
            this.stateHandler = stateHandler;
            this.config = config;
            this.userCodeClassloader = userCodeClassloader;
            this.inputSpec = inputSpec;
            this.stateRetentionTime = stateRetentionTime;

            // Initialize state
            initializeState(inputIndex);
        }

        private void initializeState(int inputIndex) {
            MapStateDescriptor<String, RowData> recordStateDesc =
                    new MapStateDescriptor<>(
                            "multi-join-record-state-" + inputIndex,
                            InternalTypeInfo.of(String.class),
                            InternalTypeInfo.of(RowData.class));

            this.operator
                    .getRuntimeContext()
                    .setKeyedStateStore(this.stateHandler.getKeyedStateStore().get());
            this.recordState = this.operator.getRuntimeContext().getMapState(recordStateDesc);
        }

        @Override
        public Iterator<RowData> getRecords() throws Exception {
            return recordState.values().iterator();
        }

        @Override
        public void addRecord(RowData record) throws Exception {
            // still hard coded, we'll use the key selector
            // TODO GUSTAVO
            String key = record.getString(0).toString();
            recordState.put(key, record);
        }

        public void retractRecord(RowData record) throws Exception {
            // still hard coded, we'll use the key selector
            String key = record.getString(0).toString();
            recordState.remove(key);
        }
    }

    public static class MultiOuterJoinStateHandler implements MultiJoinStateHandler {

        private final StreamingMultiJoinOperator operator;
        private final StreamOperatorStateHandler stateHandler;

        private final Configuration config;
        private final ClassLoader userCodeClassloader;
        private final JoinInputSideSpec inputSpec;
        private final long stateRetentionTime;

        private transient MapState<Integer, Tuple2<RowData, Integer>> recordState;

        public MultiOuterJoinStateHandler(
                int inputIndex,
                StreamingMultiJoinOperator operator,
                StreamOperatorStateHandler stateHandler,
                Configuration config,
                ClassLoader userCodeClassloader,
                JoinInputSideSpec inputSpec,
                InternalTypeInfo<RowData> recordType,
                long stateRetentionTime) {
            this.operator = operator;
            this.stateHandler = stateHandler;
            this.config = config;
            this.userCodeClassloader = userCodeClassloader;
            this.inputSpec = inputSpec;
            this.stateRetentionTime = stateRetentionTime;

            // Initialize state
            initializeState(inputIndex, recordType);
        }

        private void initializeState(int inputIndex, InternalTypeInfo<RowData> recordType) {
            TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo =
                    new TupleTypeInfo<>(recordType, Types.INT);
            MapStateDescriptor<Integer, Tuple2<RowData, Integer>> recordStateDesc =
                    new MapStateDescriptor<>(
                            "multi-join-record-state-" + inputIndex,
                            InternalTypeInfo.of(Integer.class),
                            valueTypeInfo);

            this.operator
                    .getRuntimeContext()
                    .setKeyedStateStore(this.stateHandler.getKeyedStateStore().get());
            this.recordState = this.operator.getRuntimeContext().getMapState(recordStateDesc);
        }

        @Override
        public void addRecord(RowData record) throws Exception {
            addRecord(record, 0);
        }

        public void addRecord(RowData record, int numOfAssociations) throws Exception {
            int uniqueKey = getUniqueKey(record);
            recordState.put(uniqueKey, Tuple2.of(record, numOfAssociations));
        }

        public void updateNumOfAssociations(RowData record, int numOfAssociations)
                throws Exception {
            int key = getUniqueKey(record);
            recordState.put(key, Tuple2.of(record, numOfAssociations));
        }

        public void retractRecord(RowData record) throws Exception {
            // still hard coded, we'll use the key selector
            int key = getUniqueKey(record);
            recordState.remove(key);
        }

        public Integer getRecordAssociations(RowData record) throws Exception {
            var recordWithAssociations = recordState.get(getUniqueKey(record));
            if (recordWithAssociations == null) {
                return 0;
            } else {
                return recordWithAssociations.f1;
            }
        }

        @Override
        public Iterator<RowData> getRecords() throws Exception {
            return new RecordsIterable(getRecordsAndNumOfAssociations()).iterator();
        }
        
        @Override
        public JoinRecordIterator getRecordsWithAssociations() throws Exception {
            return JoinRecordIterator.fromTupleIterator(getRecordsAndNumOfAssociations());
        }

        public Iterator<Tuple2<RowData, Integer>> getRecordsAndNumOfAssociations()
                throws Exception {
            return recordState.values().iterator();
        }

        private static int getUniqueKey(RowData record) {
            int key = Integer.parseInt(record.getString(1).toString());
            return key;
        }

        private static final class RecordsIterable implements IterableIterator<RowData> {
            private final Iterator<Tuple2<RowData, Integer>> tupleIterator;

            private RecordsIterable(Iterator<Tuple2<RowData, Integer>> tuples) {
                checkNotNull(tuples);
                this.tupleIterator = tuples;
            }

            @Override
            public Iterator<RowData> iterator() {
                return this;
            }

            @Override
            public boolean hasNext() {
                return tupleIterator.hasNext();
            }

            @Override
            public RowData next() {
                return tupleIterator.next().f0;
            }
        }
    }
}
