package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.MultiJoinStateHandlers.*;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

// TODO Gustavo we probably shouldn't be importing calcite stuff here and we have a somewhere
import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming multi-way join operator which supports inner join and left/right/full outer join. It
 * eliminates the intermediate state necessary for a chain of multiple binary joins. In other words,
 * it considerable reduces the total amount of state necessary for chained joins. As of time
 * complexity, it performs better in the worst cases where the number of records in the intermediate
 * state is large but worst than reorded binary joins when the number of records in the intermediate
 * state is small.
 */
public class StreamingMultiJoinOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingMultiJoinOperator.class);
    private static final long serialVersionUID = 1L;

    private final List<JoinInputSideSpec> inputSpecs;
    private final List<JoinRelType> joinTypes;
    private final List<JoinCondition> joinConditions;
    private final List<InternalTypeInfo<RowData>> inputTypes;
    private final MultiJoinCondition multiJoinCondition;
    private final boolean[] filterNulls;
    private final long[] stateRetentionTime;
    private final List<Input> inputs;
    private final boolean isFullOuterJoin;
    private final MultiJoinCondition[] outerJoinConditions;

    private transient List<MultiJoinStateHandler> stateHandlers;
    private transient ValueState<Long> cleanupTimeState;
    private transient TimestampedCollector<RowData> collector;
    private transient List<RowData> nullRows;

    // TODO gustavo get rid
    private final List<KeySelector<RowData, String>> dummyKeySelectors;

    /**
     * Represents the different phases of the join process.
     */
    private enum JoinPhase {
        /** Phase where we calculate match counts (associations) without emitting results */
        CALCULATE_MATCHES,
        /** Phase where we emit the actual join results */
        EMIT_RESULTS
    }

    /**
     * Constructor for the multi way joing operator
     */

    /*
    - We'll not add the input to the state directly and also not create one singleton for the input but
    - We'll iterate through all the input iterators as they are
    - We'll get rid of associations and hasMatches
    - We'll have one array for numOfMatches that always contains the number of matches to the right and we'll calculate that on the go instead
    - For every next row currentRows[depth] = allInputRecords.get(depth).next(), we also set the num of associations to 0 for this depth
    - If outerJoinConditions[depth].apply(currentRows) is true, we increase the previous depth hasMatches by 1
    - Input param is is null for these calls, in the depth == inputSpecs.size() check we also check if input != null
    - When we leave the while loop, if hasMatches for the previous depth is 0, we call recursiveMultiJoin with a null padded row for the next level instead of doing emitRow(input.getRowKind(), currentRows);
    - After that, if depth == inputId, we now call recursiveMultiJoin with the proper input row, also with current row updated to have the input at depth and with the up to date and the recalculated hasMatches array.
    - We want to rewrite emitRowWithNullPaddedSide to pad the rows that had hasMatches[i - 1] == 0
    */
    public StreamingMultiJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            List<InternalTypeInfo<RowData>> inputTypes,
            List<JoinInputSideSpec> inputSpecs,
            List<KeySelector<RowData, String>> dummyKeySelectors,
            List<JoinRelType> joinTypes,
            List<JoinCondition> joinConditions,
            MultiJoinCondition multiJoinCondition,
            boolean[] filterNulls,
            long[] stateRetentionTime,
            boolean isFullOuterJoin,
            MultiJoinCondition[] outerJoinConditions) {
        super(parameters, inputSpecs.size());
        this.inputTypes = inputTypes;
        this.inputSpecs = inputSpecs;
        this.dummyKeySelectors = dummyKeySelectors;
        this.joinTypes = joinTypes;
        this.joinConditions = joinConditions;
        this.multiJoinCondition = multiJoinCondition;
        this.filterNulls = filterNulls;
        this.stateRetentionTime = stateRetentionTime;
        this.isFullOuterJoin = isFullOuterJoin;
        this.outerJoinConditions = outerJoinConditions;
        this.inputs = new ArrayList<>(inputSpecs.size());
    }

    @Override
    public void open() throws Exception {
        super.open();
        initializeCollector();
        initializeNullRows();
        initializeStateHandlers();
        initializeCleanupState();
    }

    @Override
    public void close() throws Exception {
        closeConditions();
        super.close();
    }

    public void processElement(int inputId, StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        long timestamp = element.getTimestamp();
        processElement(inputId, input, timestamp);
    }

    private void processElement(int inputId, RowData input, long timestamp) throws Exception {
        inputId = inputId - 1; // Convert to 0-based index

        // We perform the multi-way join for the input streams
        performMultiJoin(input, inputId);

        addRecordToState(input, inputId);
        updateCleanupTime(timestamp);
    }

    private void performMultiJoin(RowData input, int inputId) throws Exception {
        if (input == null) {
            return;
        }

        int[] associations = createInitialAssociations();
        RowData[] currentRows = new RowData[inputSpecs.size()];

        recursiveMultiJoin(
                0,
                input,
                inputId,
                currentRows,
                associations,
                JoinPhase.CALCULATE_MATCHES);
    }

    private boolean recursiveMultiJoin(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            int[] associations,
            JoinPhase phase)
            throws Exception {
        if (depth == inputSpecs.size()) {
            return processJoinAtMaxDepth(depth, input, currentRows, phase);
        }

        boolean isLeftJoin = isLeftJoinAtDepth(depth);
        boolean matched = processExistingRecords(
                depth, input, inputId, currentRows,
                associations, phase, isLeftJoin);

        if (isLeftJoin && !matched && associations[depth - 1] == 0) {
            matched = processWithNullPadding(
                    depth, input, inputId, currentRows,
                    associations, phase);
        }

        if (depth == inputId) {
            matched = processInputRecord(
                    depth, input, inputId, currentRows,
                    associations);
        }

        return matched;
    }

    private boolean processJoinAtMaxDepth(
            int depth, RowData input, RowData[] currentRows, 
            JoinPhase phase) {
        
        boolean isLeftJoin = isLeftJoinAtLastLevel(depth);

        if (!isLeftJoin && !multiJoinCondition.apply(currentRows)) {
            return false;
        }

        if (phase == JoinPhase.CALCULATE_MATCHES) {
            return true;
        }

        emitRow(input.getRowKind(), currentRows);
        return true;
    }

    private boolean processExistingRecords(
            int depth, RowData input, int inputId, RowData[] currentRows,
            int[] associations, JoinPhase phase, boolean isLeftJoin) throws Exception {
        
        boolean matched = false;
        JoinRecordIterator recordIterator = stateHandlers.get(depth).getRecordsWithAssociations();

        while (recordIterator.hasNext()) {
            currentRows[depth] = recordIterator.next();

            if (isLeftJoin) {
                if (!matchesOuterCondition(depth, currentRows)) {
                    continue;
                }

                updateAssociationCount(depth, associations, shouldIncrementAssociation(phase, input));
            }

            if (isLeftJoin) {
                associations[depth] = 0;
            }

            matched = recursiveMultiJoin(
                    depth + 1, input, inputId, currentRows,
                    associations, phase);
        }

        return matched;
    }

    private boolean processWithNullPadding(
            int depth, RowData input, int inputId, RowData[] currentRows,
            int[] associations, JoinPhase phase) throws Exception {

        currentRows[depth] = nullRows.get(depth);
        return recursiveMultiJoin(depth + 1, input, inputId, currentRows, associations, phase);
    }

    private boolean processInputRecord(
            int depth, RowData input, int inputId, RowData[] currentRows,
            int[] associations) throws Exception {

        boolean matched = false;
        boolean isLeftJoin = isLeftJoinAtDepth(depth);
        RowKind inputRowKind = input.getRowKind();

        if (isUpsert(input) && isLeftJoin && associations[depth - 1] == 0) {
            matched = handleRetractBeforeInput(
                    depth, input, inputId, currentRows,
                    associations);
        }

        currentRows[depth] = input;

        if (isLeftJoin) {
            if (!matchesOuterCondition(depth, currentRows)) {
                return false;
            }
            updateAssociationCount(depth, associations, shouldIncrementAssociation(JoinPhase.EMIT_RESULTS, input));
        }

        input.setRowKind(inputRowKind);
        matched = recursiveMultiJoin(
                depth + 1, input, inputId, currentRows,
                associations, JoinPhase.EMIT_RESULTS);

        if (!isUpsert(input) && isLeftJoin && associations[depth - 1] == 0) {
            matched = handleInsertAfterInput(
                    depth, input, inputId, currentRows,
                    associations);
        }

        input.setRowKind(inputRowKind);
        return matched;
    }

    private boolean handleRetractBeforeInput(
            int depth, RowData input, int inputId, RowData[] currentRows,
            int[] associations) throws Exception {

        currentRows[depth] = nullRows.get(depth);
        RowKind originalKind = input.getRowKind();
        input.setRowKind(RowKind.DELETE);

        boolean matched = recursiveMultiJoin(
                depth + 1, input, inputId, currentRows,
                associations, JoinPhase.EMIT_RESULTS);

        input.setRowKind(originalKind);
        return matched;
    }

    private boolean handleInsertAfterInput(
            int depth, RowData input, int inputId, RowData[] currentRows,
            int[] associations) throws Exception {

        currentRows[depth] = nullRows.get(depth);
        RowKind originalKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT);

        boolean matched = recursiveMultiJoin(
                depth + 1, input, inputId, currentRows,
                associations, JoinPhase.EMIT_RESULTS);

        input.setRowKind(originalKind);
        return matched;
    }

    private void addRecordToState(RowData input, int inputId) throws Exception {
        if (isRetraction(input)) {
            stateHandlers.get(inputId).retractRecord(input);
        } else {
            if (stateHandlers.get(inputId) instanceof MultiOuterJoinStateHandler) {
                var outStateHandler = ((MultiOuterJoinStateHandler) stateHandlers.get(inputId));
                var associations = outStateHandler.getRecordAssociations(input);
                outStateHandler.addRecord(input, associations);
            } else {
                stateHandlers.get(inputId).addRecord(input);
            }
        }
    }

    private void updateCleanupTime(long timestamp) throws Exception {
        Long currentCleanupTime = cleanupTimeState.value();
        long newCleanupTime = timestamp + getMaxRetentionTime();
        if (currentCleanupTime == null || newCleanupTime > currentCleanupTime) {
            cleanupTimeState.update(newCleanupTime);
        }
    }

    private void initializeCollector() {
        this.collector = new TimestampedCollector<>(output);
    }

    private void initializeNullRows() {
        this.nullRows = new ArrayList<>(inputTypes.size());
        for (InternalTypeInfo<RowData> inputType : inputTypes) {
            this.nullRows.add(new GenericRowData(inputType.toRowType().getFieldCount()));
        }
    }

    private void initializeStateHandlers() throws Exception {
        this.stateHandlers = new ArrayList<>(inputSpecs.size());
        for (int i = 0; i < inputSpecs.size(); i++) {
            MultiJoinStateHandler handler =
                    new MultiOuterJoinStateHandler(
                            i,
                            this,
                            dummyKeySelectors.get(i),
                            this.stateHandler,
                            getOperatorConfig().getConfiguration(),
                            getUserCodeClassloader(),
                            inputSpecs.get(i),
                            inputTypes.get(i),
                            stateRetentionTime[i]);
            stateHandlers.add(handler);
            inputs.add(createInput(i + 1));
        }
    }

    private void initializeCleanupState() throws Exception {
        ValueStateDescriptor<Long> cleanupTimeDescriptor =
                new ValueStateDescriptor<>("cleanup-time", Types.LONG);
        this.cleanupTimeState = getRuntimeContext().getState(cleanupTimeDescriptor);
    }

    private void closeConditions() throws Exception {
        if (joinConditions != null) {
            for (JoinCondition condition : joinConditions) {
                condition.close();
            }
        }

        if (multiJoinCondition != null) {
            multiJoinCondition.close();
        }
    }

    private Input<RowData> createInput(int idx) {
        return new AbstractInput<RowData, RowData>(this, idx) {
            @Override
            public void processElement(StreamRecord<RowData> element) throws Exception {
                ((StreamingMultiJoinOperator) owner).processElement(idx, element);
            }
        };
    }

    private void emitRow(RowKind rowKind, RowData[] rows) {
        RowData joinedRow = rows[0];
        for (int i = 1; i < rows.length; i++) {
            joinedRow = new JoinedRowData(rowKind, joinedRow, rows[i]);
        }
        collector.collect(joinedRow);
    }

    private boolean isUpsert(RowData row) {
        return row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER;
    }

    private boolean isRetraction(RowData row) {
        return row.getRowKind() == RowKind.DELETE || row.getRowKind() == RowKind.UPDATE_BEFORE;
    }

    private boolean isLeftJoinAtDepth(int depth) {
        return depth > 0 && joinTypes.get(depth) == JoinRelType.LEFT;
    }

    private boolean isLeftJoinAtLastLevel(int depth) {
        return depth > 0 && joinTypes.get(depth - 1) == JoinRelType.LEFT;
    }

    private boolean matchesOuterCondition(int depth, RowData[] currentRows) {
        return outerJoinConditions[depth].apply(currentRows);
    }

    private void updateAssociationCount(int depth, int[] associations, boolean isUpsert) {
        if (isUpsert) {
            associations[depth - 1]++;
        } else {
            associations[depth - 1]--;
        }
    }

    private boolean shouldIncrementAssociation(JoinPhase phase, RowData input) {
        return phase == JoinPhase.CALCULATE_MATCHES || isUpsert(input);
    }

    private int[] createInitialAssociations() {
        int[] associations = new int[inputSpecs.size()];
        Arrays.fill(associations, 0);
        return associations;
    }

    private long getMaxRetentionTime() {
        long maxTime = 0;
        for (long time : stateRetentionTime) {
            maxTime = Math.max(maxTime, time);
        }
        return maxTime;
    }

    @Override
    public List<Input> getInputs() {
        return inputs;
    }
}

