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
        System.out.println(String.format("Processing element: inputId=%d, rowKind=%s, timestamp=%d", 
            inputId, input.getRowKind(), timestamp));

        // Use multi-way join condition if available, otherwise use binary joins
        if (multiJoinCondition != null) {
            System.out.println("Using multi-way join condition");
            // First perform the join without adding the record to state
            performMultiJoin(input, inputId);

            // Then add the record to state for future joins
            addRecordToState(inputId, input);
        } else {
            System.out.println("Using binary join approach");
            // For binary join approach, add to state first then perform join
            addRecordToState(inputId, input);
            performMultiBinaryJoin(input, inputId);
        }

        // todo gustavo updateCleanupTime(timestamp);
    }

    private void addRecordToState(int inputId, RowData input) throws Exception {
        System.out.println(String.format("Adding record to state: inputId=%d, rowKind=%s", 
            inputId, input.getRowKind()));
        if (input.getRowKind() == RowKind.DELETE || input.getRowKind() == RowKind.UPDATE_BEFORE) {
            System.out.println("Retracting record from state");
            stateHandlers.get(inputId).retractRecord(input);
        } else {
            if (stateHandlers.get(inputId) instanceof MultiOuterJoinStateHandler) {
                System.out.println("Adding record to outer join state handler");
                var outStateHandler = ((MultiOuterJoinStateHandler) stateHandlers.get(inputId));
                var associations = outStateHandler.getRecordAssociations(input);
                outStateHandler.addRecord(input, associations);
            } else {
                System.out.println("Adding record to regular state handler");
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
            System.out.println("Skipping null input in performMultiJoin");
            return;
        }

        System.out.println(String.format("Starting multi-join: inputId=%d, rowKind=%s", 
            inputId, input.getRowKind()));

        // Get iterables for all inputs without modifying state
        List<JoinRecordIterator> allInputRecords = getAllInputRecords();

        // Array to track number of matches for each input to the right
        int[] matches = new int[inputSpecs.size()];
        Arrays.fill(matches, 0);
        System.out.println("Initial matches array: " + Arrays.toString(matches));

        // Create a row array to build our join result
        RowData[] currentRows = new RowData[inputSpecs.size()];

        // Process the whole join
        recursiveMultiJoin(
                0,
                input,
                inputId,
                currentRows,
                allInputRecords,
                matches,
                matches.clone(),
                true,
                false);
    }

    /** Gets iterators for all inputs without creating singletons. */
    private List<JoinRecordIterator> getAllInputRecords() throws Exception {
        List<JoinRecordIterator> allInputRecords = new ArrayList<>(inputSpecs.size());
        for (int i = 0; i < inputSpecs.size(); i++) {
            JoinRecordIterator records = stateHandlers.get(i).getRecordsWithAssociations();
            allInputRecords.add(records);
        }
        return allInputRecords;
    }

    /**
     * Processes the actual input record within the recursive join. This is called after all
     * existing state records have been evaluated.
     */
    private boolean recursiveMultiJoin(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            List<JoinRecordIterator> allInputRecords,
            int[] matches,
            int[] emittedMatches,
            boolean isUpsert,
            boolean shouldEmit)
            throws Exception {
        System.out.println(String.format("recursiveMultiJoin: depth=%d, inputId=%d, isUpsert=%b, shouldEmit=%b", 
            depth, inputId, isUpsert, shouldEmit));
        System.out.println("Current rows state:");
        for (int i = 0; i < currentRows.length; i++) {
            System.out.println(String.format("  Row[%d]: %s", i, currentRows[i] != null ? 
                String.format("kind=%s, fields=%s", currentRows[i].getRowKind(), currentRows[i]) : "null"));
        }
        System.out.println("Current matches array: " + Arrays.toString(matches));
        System.out.println("Current emittedMatches array: " + Arrays.toString(emittedMatches));

        var isRetract = !isUpsert;
        var rightSide = depth == inputSpecs.size() ? depth - 1 : depth;
        var leftSide = rightSide - 1;
        var leftJoin = rightSide > 0 && joinTypes.get(rightSide) == JoinRelType.LEFT;

        var checkCondition = depth == inputSpecs.size();
        if (checkCondition) {
            System.out.println("Checking final join condition at depth=" + depth);

            // For inner joins, we don't check the condition on every level
            if (!leftJoin && !multiJoinCondition.apply(currentRows)) {
                System.out.println("Inner join condition not satisfied");
                return false;
            }

            // We're just recalculating num of matches
            if (!shouldEmit) {
                System.out.println("Skipping emission, just recalculating matches");
                return true;
            }

            // Retract previous padded row
            if (isUpsert && leftJoin) {
                System.out.println("Retracting previous padded row for left join");
                emitRetractPaddedRow(
                        input.getRowKind(), RowKind.DELETE, currentRows, emittedMatches, inputId);
            }

            // Emit the matching row for both upserts and retractions
            System.out.println("Emitting matching row");
            emitRow(input.getRowKind(), currentRows);

            // Emit a padded row
            if (isRetract && leftJoin) {
                System.out.println("Emitting padded row for left join retraction");
                emitInsertPaddedRow(
                        input.getRowKind(), RowKind.INSERT, currentRows, emittedMatches, inputId);
            }

            return true;
        }

        boolean depthMatched = false;
        boolean isLeftJoin = depth > 0 && joinTypes.get(depth) == JoinRelType.LEFT;

        System.out.println(String.format("Processing depth=%d, isLeftJoin=%b", depth, isLeftJoin));

        // For other depths, process all records from state
        JoinRecordIterator recordIterator = stateHandlers.get(depth).getRecordsWithAssociations();

        // Process each record at this depth
        while (recordIterator.hasNext()) {
            // Get the next record at this depth
            currentRows[depth] = recordIterator.next();
            System.out.println(String.format("Processing record at depth %d: %s", 
                depth, currentRows[depth] != null ? 
                String.format("kind=%s, fields=%s", currentRows[depth].getRowKind(), currentRows[depth]) : "null"));

            // For outer joins, check the condition
            if (isLeftJoin) {
                // If condition doesn't match, skip this record
                boolean conditionMatches = outerJoinConditions[depth].apply(currentRows);
                if (!conditionMatches) {
                    System.out.println("Left join condition not satisfied at depth=" + depth);
                    continue;
                }

                // If condition matches or we're just recalculating num of matches, we only
                // increase the number of matches
                if (isUpsert) {
                    matches[depth - 1]++;
                    System.out.println(String.format("Incremented matches for depth=%d, new count=%d, matches array: %s",
                        depth-1, matches[depth-1], Arrays.toString(matches)));
                } else {
                    matches[depth - 1]--;
                    System.out.println(String.format("Decremented matches for depth=%d, new count=%d, matches array: %s",
                        depth-1, matches[depth-1], Arrays.toString(matches)));
                }
            }

            // Recursively continue the join
            // Reset the match count for this depth
            if (leftJoin) {
                matches[depth] = 0;
                System.out.println(String.format("Reset matches for depth=%d, matches array: %s", 
                    depth, Arrays.toString(matches)));
            }
            depthMatched =
                    recursiveMultiJoin(
                            depth + 1,
                            input,
                            inputId,
                            currentRows,
                            allInputRecords,
                            matches,
                            emittedMatches,
                            isUpsert,
                            shouldEmit);

            if (depthMatched) {
                emittedMatches = matches.clone();
                System.out.println("Depth matched at depth=" + depth + ", updated emittedMatches: " + Arrays.toString(emittedMatches));
            }
        }

        // If we have no matches, we try with null padding now
        if (isLeftJoin && !depthMatched && matches[depth - 1] == 0) {
            System.out.println("No matches found at depth=" + depth + ", trying null padding");
            System.out.println("Current matches array: " + Arrays.toString(matches));
            // There were no matches, we now try with null padding
            currentRows[depth] = nullRows.get(depth);
            System.out.println(String.format("Null padded row at depth %d: %s", 
                depth, currentRows[depth] != null ? 
                String.format("kind=%s, fields=%s", currentRows[depth].getRowKind(), currentRows[depth]) : "null"));

            // We have to call the recursive join again with the null-padded row to have a correct
            // numOfMatches array
            depthMatched =
                    recursiveMultiJoin(
                            depth + 1,
                            input,
                            inputId,
                            currentRows,
                            allInputRecords,
                            matches,
                            emittedMatches,
                            isUpsert,
                            shouldEmit);

            if (depthMatched) {
                emittedMatches = matches.clone();
                System.out.println("Null padding matched at depth=" + depth + ", updated emittedMatches: " + Arrays.toString(emittedMatches));
            }
        }

        // Now we'll perform the join with the actual input record which is what we're looking for
        // ACTUAL JOIN
        if (depth == inputId) {
            System.out.println("depth == inputId");
            System.out.println("Processing input record at depth=" + depth);
            currentRows[depth] = input;
            System.out.println(String.format("Input record at depth %d: %s", 
                depth, currentRows[depth] != null ? 
                String.format("kind=%s, fields=%s", currentRows[depth].getRowKind(), currentRows[depth]) : "null"));

            // If condition doesn't match, skip this record
            if (isLeftJoin) {
                // If condition doesn't match, skip this record
                boolean conditionMatches = outerJoinConditions[depth].apply(currentRows);
                if (!conditionMatches) {
                    System.out.println("depth == inputId: Left join condition not satisfied at depth=" + depth);
                    return false;
                }

                /*// If condition matches or we're just recalculating num of matches, we only
                // increase the number of matches
                if (isUpsert) {
                    matches[depth - 1]++;
                    System.out.println(String.format("depth == inputId: Incremented matches for depth=%d, new count=%d, matches array: %s",
                            depth-1, matches[depth-1], Arrays.toString(matches)));
                } else {
                    matches[depth - 1]--;
                    System.out.println(String.format("depth == inputId: Decremented matches for depth=%d, new count=%d, matches array: %s",
                            depth-1, matches[depth-1], Arrays.toString(matches)));
                }*/
            }

            // do we need not to update num of matches now?
            boolean inputIsUpsert =
                    input.getRowKind() == RowKind.INSERT
                            || input.getRowKind() == RowKind.UPDATE_AFTER;
            depthMatched =
                    recursiveMultiJoin(
                            depth + 1,
                            input,
                            inputId,
                            currentRows,
                            allInputRecords,
                            matches,
                            emittedMatches,
                            inputIsUpsert,
                            true);

            if (depthMatched) {
                emittedMatches = matches.clone();
                System.out.println("Input record matched at depth=" + depth + ", updated emittedMatches: " + Arrays.toString(emittedMatches));
            }
        }

        return depthMatched;
    }

    /**
     * Emits the row with padding for inputs that had no matches. This version uses numOfMatches to
     * determine which inputs need padding.
     */
    private void emitRowWithNullPaddedRows(RowKind rowKind, RowData[] rows, int[] numOfMatches) {
        // Clone the rows to avoid modifying the original
        RowData[] paddedRows = rows.clone();

        // Pad with nulls for any input that had no matches (except the first)
        for (int i = 1; i < paddedRows.length; i++) {
            if (numOfMatches[i - 1] == 0 && joinTypes.get(i) != JoinRelType.INNER) {
                paddedRows[i] = nullRows.get(i);
            }
        }

        // Build the joined row by progressively joining the inputs
        RowData joinedRow = paddedRows[0];
        for (int i = 1; i < paddedRows.length; i++) {
            joinedRow = new JoinedRowData(rowKind, joinedRow, paddedRows[i]);
        }
        collector.collect(joinedRow);
    }

    /**
     * Recursively builds a cartesian product of all already filtered by key inputs and checks the
     * join condition for all of them in one go. This is a depth-first approach to building all
     * possible combinations of rows.
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
        System.out.println("Emitting row: rowKind=" + rowKind);
        System.out.println("Row contents:");
        for (int i = 0; i < rows.length; i++) {
            System.out.println(String.format("  Row[%d]: %s", i, rows[i] != null ? 
                String.format("kind=%s, fields=%s", rows[i].getRowKind(), rows[i]) : "null"));
        }
        // Build the joined row by progressively joining the inputs
        RowData joinedRow = rows[0];
        for (int i = 1; i < rows.length; i++) {
            joinedRow = new JoinedRowData(rowKind, joinedRow, rows[i]);
        }
        collector.collect(joinedRow);
    }

    /**
     * Creates and emits a row with null padding for positions where no matches were found. For each
     * position with matches[pos] = 0, the position pos + 1 will be padded with null.
     *
     * @param newRowKind The RowKind to use for the emitted row (INSERT or DELETE)
     * @param currentRows The current row array to be padded
     * @param emittedMatches Array tracking matches for each position
     * @throws Exception if any error occurs during emission
     */
    private void emitRetractPaddedRow(
            RowKind origRowKind,
            RowKind newRowKind,
            RowData[] currentRows,
            int[] emittedMatches,
            int inputId)
            throws Exception {
        System.out.println(String.format("Emitting retract padded row: origRowKind=%s, newRowKind=%s, inputId=%d", 
            origRowKind, newRowKind, inputId));
        System.out.println("Current rows state:");
        for (int i = 0; i < currentRows.length; i++) {
            System.out.println(String.format("  Row[%d]: %s", i, currentRows[i] != null ? 
                String.format("kind=%s, fields=%s", currentRows[i].getRowKind(), currentRows[i]) : "null"));
        }

        if (currentRows == null || emittedMatches == null) {
            System.out.println("Skipping retract padded row - null inputs");
            return;
        }

        // Create a copy of currentRows to avoid modifying the original
        RowData[] paddedRows = currentRows.clone();

        // Track if any padding was applied (to check if rows differ)
        boolean rowsModified = false;
        // The outmost left join should determine if an emit is necessary, we only emit if
        boolean shouldEmit = false;

        for (int i = inputId; i < emittedMatches.length; i++) {
            if (i == 0) {
                break;
            }
            var matches = outerJoinConditions[i].apply(paddedRows);

            var leftAssociations = emittedMatches[i - 1];
            if (leftAssociations == 0) {
                // Pad position pos + 1 with null
                paddedRows[i] = nullRows.get(i);
                System.out.println("Padding position " + i + " with null");
                shouldEmit = true;
                // Check if this actually modified the row (only mark as modified if we changed
                // something)
                if (!paddedRows[i].equals(currentRows[i])) {
                    rowsModified = true;
                }
            } else if (leftAssociations > 0 && !matches) {
                shouldEmit = false;
            }
        }

        // Check if the padded row is different from the original and has at least one non-null
        // value
        boolean hasNonNullRow = false;
        for (RowData row : paddedRows) {
            if (row != null) {
                hasNonNullRow = true;
                break;
            }
        }

        // Only emit if rows were modified and we have at least one non-null row
        if (shouldEmit && rowsModified && hasNonNullRow) {
            System.out.println("Emitting retract padded row - rows modified and has non-null values");
            emitRow(newRowKind, paddedRows);
        } else {
            System.out.println("Skipping retract padded row - no modifications or all nulls");
        }
    }

    private void emitInsertPaddedRow(
            RowKind origRowKind,
            RowKind newRowKind,
            RowData[] currentRows,
            int[] emittedMatches,
            int inputId)
            throws Exception {
        System.out.println(String.format("Emitting insert padded row: origRowKind=%s, newRowKind=%s, inputId=%d",
            origRowKind, newRowKind, inputId));
        System.out.println("Current rows state:");
        for (int i = 0; i < currentRows.length; i++) {
            System.out.println(String.format("  Row[%d]: %s", i, currentRows[i] != null ? 
                String.format("kind=%s, fields=%s", currentRows[i].getRowKind(), currentRows[i]) : "null"));
        }

        if (currentRows == null || emittedMatches == null) {
            System.out.println("Skipping insert padded row - null inputs");
            return;
        }

        // Create a copy of currentRows to avoid modifying the original
        RowData[] paddedRows = currentRows.clone();

        // Track if any padding was applied
        boolean rowsModified = false;
        // Flag to determine if we should emit based on left join column rule
        boolean shouldEmit = false;

        if (inputId == 0) {
            System.out.println("Skipping insert padded row - inputId is 0");
            return;
        }

        var leftAssociations = emittedMatches[inputId - 1];
        if (leftAssociations > 1) {
            System.out.println("Skipping insert padded row - too many matches at depth " + (inputId-1));
            return;
        } else if (leftAssociations == 1) {
            paddedRows[inputId] = nullRows.get(inputId);
            rowsModified = true;
            shouldEmit = true;
            System.out.println("Padding position " + inputId + " with null - single match");
        } else {
            System.out.println("Invalid state: deleting record which doesn't exist");
            throw new RuntimeException(
                    "Should not happen: we are deleting a record which doesn't exist");
        }

        for (int i = inputId + 1; i < paddedRows.length; i++) {
            var matches = outerJoinConditions[i].apply(paddedRows);

            leftAssociations = emittedMatches[i - 1];
            if (leftAssociations == 1 && !matches) {
                paddedRows[i] = nullRows.get(i);
                shouldEmit = true;
                System.out.println("Padding position " + i + " with null - no match");
            } else if (leftAssociations > 1) {
                shouldEmit = false;
                System.out.println("Skipping padding at position " + i + " - too many matches");
            }

            // Check if this actually modified the row
            if (currentRows[i] != null && !paddedRows[i].equals(currentRows[i])) {
                rowsModified = true;
            }
        }

        // Check if the padded row has at least one non-null value
        boolean hasNonNullRow = false;
        for (RowData row : paddedRows) {
            if (row != null) {
                hasNonNullRow = true;
                break;
            }
        }

        // Only emit if:
        // 1. Rows were modified
        // 2. We have at least one non-null row
        // 3. The shouldEmit flag is true (we didn't pad a non-left-join column)
        if (rowsModified && hasNonNullRow && shouldEmit) {
            System.out.println("Emitting insert padded row - all conditions met");
            emitRow(newRowKind, paddedRows);
        } else {
            System.out.println(String.format("Skipping insert padded row - conditions not met: modified=%b, hasNonNull=%b, shouldEmit=%b", 
                rowsModified, hasNonNullRow, shouldEmit));
        }
    }
}
