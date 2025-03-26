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
import java.util.Collections;
import java.util.Iterator;
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
     * Constructor that supports binary join conditions, a multi-way join condition, and outer join
     * conditions. If multiJoinCondition is provided, it will be used instead of binary join
     * conditions.
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

        // Initialize collector
        this.collector = new TimestampedCollector<>(output);

        // Initialize null rows for outer joins
        this.nullRows = new ArrayList<>(inputTypes.size());
        for (InternalTypeInfo<RowData> inputType : inputTypes) {
            this.nullRows.add(new GenericRowData(inputType.toRowType().getFieldCount()));
        }

        // Initialize state handlers for each input
        this.stateHandlers = new ArrayList<>(inputSpecs.size());

        // Initialize inputs
        for (int i = 0; i < inputSpecs.size(); i++) {
            MultiJoinStateHandler handler;
            // TODO GUSTAVO join type we need to have the join types here

            // if (outerJoinConditions[i] == null) {
            //  todo gustavo we still right nad outer here
            if (i + 1 < inputSpecs.size() && joinTypes.get(i + 1) == JoinRelType.LEFT) {
                handler =
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
            } else {
                // create outer handler
                handler =
                        new MultiJoinHasUniqueKeyStateHandler(
                                i,
                                this,
                                dummyKeySelectors.get(i),
                                this.stateHandler,
                                getOperatorConfig().getConfiguration(),
                                getUserCodeClassloader(),
                                inputSpecs.get(i),
                                stateRetentionTime[i]);
            }
            stateHandlers.add(handler);
            inputs.add(createInput(i + 1));
        }

        // Initialize cleanup time state
        ValueStateDescriptor<Long> cleanupTimeDescriptor =
                new ValueStateDescriptor<>("cleanup-time", Types.LONG);
        // todo this.cleanupTimeState = getRuntimeContext().getState(cleanupTimeDescriptor);
    }

    public void processElement(int inputId, StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        long timestamp = element.getTimestamp();

        processElement(inputId, input, timestamp);
    }

    private void processElement(int inputId, RowData input, long timestamp) throws Exception {
        inputId = inputId - 1; // Convert to 0-based index

        // Use multi-way join condition if available, otherwise use binary joins
        if (multiJoinCondition != null) {
            // First perform the join without adding the record to state
            performMultiJoin(input, inputId);

            // Then add the record to state for future joins
            addRecordToState(inputId, input);
        } else {
            // For binary join approach, add to state first then perform join
            addRecordToState(inputId, input);
            performMultiBinaryJoin(input, inputId);
        }

        // todo gustavo updateCleanupTime(timestamp);
    }

    private void addRecordToState(int inputId, RowData input) throws Exception {
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

    /**
     * Performs a multi-way join by progressively joining pairs of inputs using binary join
     * conditions. This approach builds the join result incrementally by: 0. This is a hash join:
     * we're only joining records for each input with matching keys 1. Starting with records from
     * the first input 2. Joining with the second input to produce intermediate results 3.
     * Progressively joining intermediate results with each subsequent input 4. Creating new
     * JoinedRowData objects at each step to represent partial results 5. Applying the appropriate
     * binary join condition at each step 6. Terminating early if any join step produces empty
     * results 7. We store one set of intermediate results in memory and keep updating it
     */
    private void performMultiBinaryJoin(RowData input, int inputId) throws Exception {
        // Start with initial records from first input
        List<RowData> intermediateResults = new ArrayList<>();
        collectRecords(
                inputId == 0
                        ? Collections.singleton(input).iterator()
                        : stateHandlers.get(0).getRecords(),
                intermediateResults);

        // Progressive join with each subsequent input
        for (int i = 1; i < inputSpecs.size() && !intermediateResults.isEmpty(); i++) {
            List<RowData> nextResults = new ArrayList<>();
            JoinCondition condition = joinConditions.get(i);

            // Get records from current input
            Iterator<RowData> otherSideRecords =
                    i == inputId
                            ? Collections.singleton(input).iterator()
                            : stateHandlers.get(i).getRecords();

            // Join each left record with matching right records
            for (RowData left : intermediateResults) {
                while (otherSideRecords.hasNext()) {
                    var right = otherSideRecords.next();
                    if (condition.apply(left, right)) {
                        var outRow = new JoinedRowData(left.getRowKind(), left, right);
                        // If we're not at the last input, store the joined row for further joining
                        if (i < inputSpecs.size() - 1) {
                            nextResults.add(outRow);
                        } else {
                            // If we're at the last input, emit the final joined row
                            collector.collect(outRow);
                        }
                    }
                }
            }
            intermediateResults = nextResults;
        }
    }

    /**
     * Performs a multi-way join using a single MultiJoinCondition that evaluates all join
     * conditions at once. This approach can be more efficient than the progressive binary join
     * because: 1. This is a hash join: we're only joining records for each input with matching keys
     * 2. It avoids creating intermediate joined rows 3. It can evaluate complex conditions across
     * all inputs at once 4. It can short-circuit evaluation when any condition fails
     */
    private void performMultiJoin(RowData input, int inputId) throws Exception {
        if (input == null) {
            return;
        }

        // Array to track number of matches for each input to the right
        int[] matches = new int[inputSpecs.size()];
        Arrays.fill(matches, 0);

        // Create a row array to build our join result
        RowData[] currentRows = new RowData[inputSpecs.size()];

        // Process the whole join
        recursiveMultiJoin(
                0,
                input,
                inputId,
                currentRows,
                matches,
                true,
                false);
    }

    /**
     * Recursively processes join operations, building rows depth by depth and tracking matches.
     */
    private boolean recursiveMultiJoin(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            int[] matches,
            boolean isUpsert,
            boolean shouldEmit)
            throws Exception {
        // If we've reached the maximum depth, we can emit the row if conditions are met
        if (depth == inputSpecs.size()) {
            return processJoinAtMaxDepth(depth, input, currentRows, isUpsert, shouldEmit);
        }

        boolean isLeftJoin = isLeftJoinAtDepth(depth);
        boolean depthMatched = processExistingRecords(
                depth, input, inputId, currentRows,
                matches, isUpsert, shouldEmit, isLeftJoin);

        // If we have no matches with existing records, try with null padding for left joins
        if (isLeftJoin && !depthMatched && matches[depth - 1] == 0) {
            depthMatched = processWithNullPadding(
                    depth, input, inputId, currentRows,
                    matches, isUpsert, shouldEmit);
        }

        // Process the actual input record if we're at the right depth
        if (depth == inputId) {
            depthMatched = processInputRecord(
                    depth, input, inputId, currentRows,
                    matches, isUpsert);
        }

        return depthMatched;
    }

    /**
     * Process the join when we've reached the maximum depth.
     */
    private boolean processJoinAtMaxDepth(
            int depth, RowData input, RowData[] currentRows, 
            boolean isUpsert, boolean shouldEmit) {
        
        // Check if this is a left join at the last level
        boolean isLeftJoin = depth > 0 && joinTypes.get(depth - 1) == JoinRelType.LEFT;

        // For inner joins, check the condition
        if (!isLeftJoin && !multiJoinCondition.apply(currentRows)) {
            return false;
        }

        // If we're just calculating matches and not emitting, return success
        if (!shouldEmit) {
            return true;
        }

        // Emit the matching row 
        emitRow(input.getRowKind(), currentRows);
        return true;
    }

    /**
     * Process existing records at the current depth.
     */
    private boolean processExistingRecords(
            int depth, RowData input, int inputId, RowData[] currentRows,
            int[] matches, boolean isUpsert, boolean shouldEmit, boolean isLeftJoin) throws Exception {
        
        boolean depthMatched = false;
        JoinRecordIterator recordIterator = stateHandlers.get(depth).getRecordsWithAssociations();

        // Process each record at this depth
        while (recordIterator.hasNext()) {
            // Get the next record at this depth
            currentRows[depth] = recordIterator.next();

            // For outer joins, check the condition
            if (isLeftJoin) {
                if (!checkJoinCondition(depth, currentRows)) {
                    continue;
                }
                
                // Update match counts if condition is satisfied
                updateMatchCount(depth, matches, isUpsert);
            }

            // Reset match count and continue recursively
            if (isLeftJoin) {
                matches[depth] = 0;
            }
            
            boolean matched = recursiveMultiJoin(
                    depth + 1, input, inputId, currentRows,
                    matches, isUpsert, shouldEmit);

            if (matched) {
                depthMatched = true;
            }
        }
        
        return depthMatched;
    }

    /**
     * Process with null padding when no matches are found in a left join.
     */
    private boolean processWithNullPadding(
            int depth, RowData input, int inputId, RowData[] currentRows,
            int[] matches, boolean isUpsert, boolean shouldEmit) throws Exception {
        
        // There were no matches, we try with null padding
        currentRows[depth] = nullRows.get(depth);

        // Call recursive join again with null-padded row to get correct matches
        boolean depthMatched = recursiveMultiJoin(
                depth + 1, input, inputId, currentRows,
                matches, isUpsert, shouldEmit);
        
        return depthMatched;
    }

    /**
     * Process the actual input record at the appropriate depth.
     */
    private boolean processInputRecord(
            int depth, RowData input, int inputId, RowData[] currentRows,
            int[] matches, boolean isUpsert) throws Exception {
        
        boolean depthMatched = false;
        boolean isLeftJoin = isLeftJoinAtDepth(depth);
        boolean inputIsUpsert = isUpsert(input);
        RowKind inputRowKind = input.getRowKind();
        
        // Handle retraction of previous null-padded results if needed
        if (inputIsUpsert && isLeftJoin && matches[depth - 1] == 0) {
            depthMatched = handleRetractBeforeInput(
                    depth, input, inputId, currentRows,
                    matches);
        }

        // Process with the actual input
        currentRows[depth] = input;
        
        // Check left join condition if needed
        if (isLeftJoin) {
            if (!checkJoinCondition(depth, currentRows)) {
                return false;
            }
            
            // Update match counts if condition is satisfied
            updateMatchCount(depth, matches, inputIsUpsert);
        }

        // Continue recursive join with the actual input
        input.setRowKind(inputRowKind);
        depthMatched = recursiveMultiJoin(
                depth + 1, input, inputId, currentRows,
                matches, inputIsUpsert, true);

        // Handle insertion of new null-padded results if needed
        if (!inputIsUpsert && isLeftJoin && matches[depth - 1] == 0) {
            depthMatched = handleInsertAfterInput(
                    depth, input, inputId, currentRows,
                    matches);
        }

        // Restore original row kind
        input.setRowKind(inputRowKind);
        
        return depthMatched;
    }

    /**
     * Handle retraction of previous null-padded results before processing input.
     */
    private boolean handleRetractBeforeInput(
            int depth, RowData input, int inputId, RowData[] currentRows,
            int[] matches) throws Exception {
        
        // Set null padding for retraction
        currentRows[depth] = nullRows.get(depth);
        
        // Temporarily change row kind for retraction
        RowKind originalKind = input.getRowKind();
        input.setRowKind(RowKind.DELETE);
        
        // Process the retraction
        boolean depthMatched = recursiveMultiJoin(
                depth + 1, input, inputId, currentRows,
                matches, false, true);
        
        // Restore original row kind
        input.setRowKind(originalKind);
        
        return depthMatched;
    }

    /**
     * Handle insertion of new null-padded results after processing input.
     */
    private boolean handleInsertAfterInput(
            int depth, RowData input, int inputId, RowData[] currentRows,
            int[] matches) throws Exception {
        
        // Set null padding for insertion
        currentRows[depth] = nullRows.get(depth);
        
        // Temporarily change row kind for insertion
        RowKind originalKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT);
        
        // Process the insertion
        boolean depthMatched = recursiveMultiJoin(
                depth + 1, input, inputId, currentRows,
                matches, true, true);
        
        // Restore original row kind
        input.setRowKind(originalKind);
        
        return depthMatched;
    }

    /**
     * Check if the join condition is satisfied for the rows at the given depth.
     * 
     * @param depth the current processing depth
     * @param currentRows the array of rows being processed
     * @return true if the join condition is satisfied, false otherwise
     */
    private boolean checkJoinCondition(int depth, RowData[] currentRows) {
        return outerJoinConditions[depth].apply(currentRows);
    }
    
    /**
     * Update the match count for the given depth based on the operation type.
     * 
     * @param depth the current processing depth
     * @param matches the array of match counts to update
     * @param isUpsert true if this is an upsert operation, false for retractions
     */
    private void updateMatchCount(int depth, int[] matches, boolean isUpsert) {
        if (isUpsert) {
            matches[depth - 1]++;
        } else {
            matches[depth - 1]--;
        }
    }

    /**
     * Collect records from an iterator into a target list.
     */
    private void collectRecords(Iterator<RowData> records, List<RowData> target) throws Exception {
        while (records.hasNext()) {
            target.add(records.next());
        }
    }

    @Override
    public List<Input> getInputs() {
        return inputs;
    }

    private Input<RowData> createInput(int idx) {
        return new AbstractInput<RowData, RowData>(this, idx) {
            @Override
            public void processElement(StreamRecord<RowData> element) throws Exception {
                ((StreamingMultiJoinOperator) owner).processElement(idx, element);
            }
        };
    }

    private void updateCleanupTime(long timestamp) throws Exception {
        Long currentCleanupTime = cleanupTimeState.value();
        long newCleanupTime = timestamp + getMaxRetentionTime();
        if (currentCleanupTime == null || newCleanupTime > currentCleanupTime) {
            cleanupTimeState.update(newCleanupTime);
        }
    }

    private long getMaxRetentionTime() {
        long maxTime = 0;
        for (long time : stateRetentionTime) {
            maxTime = Math.max(maxTime, time);
        }
        return maxTime;
    }

    @Override
    public void close() throws Exception {
        if (joinConditions != null) {
            for (JoinCondition condition : joinConditions) {
                condition.close();
            }
        }

        if (multiJoinCondition != null) {
            multiJoinCondition.close();
        }

        super.close();
    }

    /** Emits a row with the specified row kind. */
    private void emitRow(RowKind rowKind, RowData[] rows) {
        // Build the joined row by progressively joining the inputs
        RowData joinedRow = rows[0];
        for (int i = 1; i < rows.length; i++) {
            joinedRow = new JoinedRowData(rowKind, joinedRow, rows[i]);
        }
        collector.collect(joinedRow);
    }

    /**
     * Check if a row is an upsert operation (INSERT or UPDATE_AFTER).
     */
    private boolean isUpsert(RowData row) {
        return row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER;
    }

    /**
     * Check if a row is a retraction operation (DELETE or UPDATE_BEFORE).
     */
    private boolean isRetraction(RowData row) {
        return row.getRowKind() == RowKind.DELETE || row.getRowKind() == RowKind.UPDATE_BEFORE;
    }

    /**
     * Check if the join at a specific depth is a left join.
     */
    private boolean isLeftJoinAtDepth(int depth) {
        return depth > 0 && joinTypes.get(depth) == JoinRelType.LEFT;
    }
}

