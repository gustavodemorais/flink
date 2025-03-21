package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.calcite.rel.core.JoinRelType; // todo gustavo I probably shouldn't import this here?
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

    /**
     * Constructor that supports binary join conditions, a multi-way join condition, and outer join conditions.
     * If multiJoinCondition is provided, it will be used instead of binary join conditions.
     */
    public StreamingMultiJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            List<InternalTypeInfo<RowData>> inputTypes,
            List<JoinInputSideSpec> inputSpecs,
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

            //if (outerJoinConditions[i] == null) {
            //  todo gustavo we still right nad outer here
            if (i + 1 < inputSpecs.size() && joinTypes.get(i + 1) == JoinRelType.LEFT) {
                handler =
                        new MultiOuterJoinStateHandler(
                                i,
                                this,
                                this.stateHandler,
                                getOperatorConfig().getConfiguration(),
                                getUserCodeClassloader(),
                                inputSpecs.get(i),
                                inputTypes.get(i),
                                stateRetentionTime[i]);
            }  else {
                // create outer handler
                handler =
                        new MultiJoinHasUniqueKeyStateHandler(
                                i,
                                this,
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

        addRecordToState(inputId, input);

        // Use multi-way join condition if available, otherwise use binary joins
        if (multiJoinCondition != null) {
            performMultiJoin(input, inputId);
        } else {
            performMultiBinaryJoin(input, inputId);
        }

        // todo gustavo updateCleanupTime(timestamp);
    }

    private void addRecordToState(int inputId, RowData input) throws Exception {
        if (input.getRowKind() == RowKind.DELETE || input.getRowKind() == RowKind.UPDATE_BEFORE) {
            stateHandlers.get(inputId).retractRecord(input);
        } else {
            if (stateHandlers.get(inputId) instanceof MultiOuterJoinStateHandler) {
                var outStateHandler = ((MultiOuterJoinStateHandler) stateHandlers.get(inputId));
                var numOfAssociations = outStateHandler.getRecordAssociations(input);
                outStateHandler.addRecord(input, numOfAssociations);
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
        // Get iterables for all inputs
        List<JoinRecordIterator> allInputRecords = getAllInputRecordsWithAssociations(inputId, input);

        // Track which inputs had matches for outer join handling
        boolean[] hasMatches = new boolean[inputSpecs.size()];
        Arrays.fill(hasMatches, false);

        // Perform a cartesian product with condition check across all inputs
        recursiveMultiJoin(0, input, inputId, new RowData[inputSpecs.size()],
                           allInputRecords, hasMatches);
    }

    private void recursiveMultiJoin(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            List<JoinRecordIterator> allInputRecords,
            boolean[] hasMatches)
            throws Exception {
        if (depth == inputSpecs.size()) {
            var isUpsert = input.getRowKind() == RowKind.UPDATE_AFTER || input.getRowKind() == RowKind.INSERT;
            var rightSide = depth -1 ;
            var leftSide = rightSide - 1;
            var inputIsLeft = inputId < rightSide;
            var inputIsRight = !inputIsLeft;
            var leftJoin = joinTypes.get(rightSide) == JoinRelType.LEFT;
            int associations =
                    allInputRecords.get(leftSide).getRecordWithAssociations().f1;
            Arrays.fill(hasMatches, true);
            if (multiJoinCondition.apply(currentRows)) {
                // Retract previous padded row
                var appendedRight = inputIsRight && associations == 0;
                if (leftJoin && appendedRight) {
                    emitRowWithNullPaddedInput(RowKind.DELETE, rightSide, currentRows);
                }

                // Emit the matching row for both upserts and retractions
                associations = updateAssociations(currentRows, isUpsert, associations, leftSide);
                emitRow(input.getRowKind(), currentRows);

                // Emit a padded row
                var deletedRightSide = associations == 0 && !inputIsLeft;
                if (leftJoin && deletedRightSide) {
                    emitRowWithNullPaddedInput(RowKind.INSERT, rightSide, currentRows);
                }

            } else if (leftJoin) {
                currentRows[rightSide] = nullRows.get(rightSide);

                emitRow(input.getRowKind(), currentRows);
                hasMatches[rightSide] = true;
            }

            return;
        }

        // For the current depth, iterate over all possible rows
        var matched = false;
        while (allInputRecords.get(depth).hasNext()) {
            currentRows[depth] = allInputRecords.get(depth).next();

            // if we get to the last one and there are no matches we have to pad the row
            if (joinTypes.get(depth) != JoinRelType.INNER) {
                if (joinTypes.get(depth) == JoinRelType.LEFT && outerJoinConditions[depth].apply(currentRows)) {
                    matched = true;
                } else {
                    continue;
                }
            }

            recursiveMultiJoin(depth + 1, input, inputId, currentRows, allInputRecords, hasMatches);
        }

        if (joinTypes.get(depth) != JoinRelType.INNER) {
            if (joinTypes.get(depth) == JoinRelType.LEFT && !matched) {
                currentRows[depth] = nullRows.get(depth);
                recursiveMultiJoin(depth + 1, input, inputId, currentRows, allInputRecords, hasMatches);
            }
        }

        // if we reach the last one and there are not matches we have to pad the row
    }

    private int updateAssociations(
            RowData[] currentRows,
            boolean isUpsert,
            int associationsLeft,
            int leftSide) throws Exception {
        if (isUpsert) {
            associationsLeft++;
        } else {
            associationsLeft--;
        }
        if (stateHandlers.get(leftSide) instanceof MultiOuterJoinStateHandler) {
            var outStateHandler = ((MultiOuterJoinStateHandler) stateHandlers.get(leftSide));
            outStateHandler.updateNumOfAssociations(currentRows[leftSide], associationsLeft);
        }
        return associationsLeft;
    }

    private List<JoinRecordIterator> getAllInputRecordsWithAssociations(int inputId, RowData input) throws Exception {
        List<JoinRecordIterator> allInputRecords = new ArrayList<>(inputSpecs.size());
        for (int i = 0; i < inputSpecs.size(); i++) {
            if (i == inputId) {
                // keep the number of associations if the input exists
                int numOfAssociations = 0;
                // change this so I don't have to do the instance of
                if (stateHandlers.get(inputId) instanceof MultiOuterJoinStateHandler) {
                    var outStateHandler = ((MultiOuterJoinStateHandler) stateHandlers.get(inputId));
                    numOfAssociations = outStateHandler.getRecordAssociations(input);
                }

                allInputRecords.add(JoinRecordIterator.forSingleRecord(new Tuple2<>(input, numOfAssociations)));
            } else {
                JoinRecordIterator records = stateHandlers.get(i).getRecordsWithAssociations();
                allInputRecords.add(records);
            }
        }
        return allInputRecords;
    }

    private void emitRow(RowKind rowKind, RowData[] rows) {
        // Build the joined row by progressively joining the inputs
        RowData joinedRow = rows[0];
        for (int i = 1; i < rows.length; i++) {
            joinedRow = new JoinedRowData(rowKind, joinedRow, rows[i]);
        }
        collector.collect(joinedRow);
    }

    private void emitRowWithNullPaddedInput(RowKind rowKind, int inputId, RowData[] rows) {
        var paddedRows = nullPadInput(inputId, rows);
        // Build the joined row by progressively joining the inputs
        RowData joinedRow = paddedRows[0];
        for (int i = 1; i < paddedRows.length; i++) {
            joinedRow = new JoinedRowData(rowKind, joinedRow, paddedRows[i]);
        }
        collector.collect(joinedRow);
    }

    private RowData[] nullPadInput(int inputId, RowData[] rows) {
        var paddedRows = rows.clone();
        paddedRows[inputId] = nullRows.get(inputId);
        return paddedRows;
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
}
