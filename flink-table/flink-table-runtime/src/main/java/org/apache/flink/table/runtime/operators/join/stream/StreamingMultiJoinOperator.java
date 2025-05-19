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

package org.apache.flink.table.runtime.operators.join.stream;

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
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.state.MultiJoinStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.MultiJoinStateViews;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Streaming multi-way join operator which supports inner join and left outer join, right joins are
 * transformed into left joins by the optimizer. It only supports a combination of joins that joins
 * on at least one common column due to partitioning. It eliminates the intermediate state necessary
 * for a chain of multiple binary joins. In other words, it reduces the total amount of state
 * necessary for chained joins. As of time complexity, it performs better for the worst binary joins
 * cases, where the number of records in the intermediate state is large. Binary joins perform
 * better if they are optimally ordered, updates come mostly for the table on the right and the
 * query uses primary keys (the intermediate state for a specific join key is small).
 *
 * <p>Performs the multi-way join logic recursively. This method drives the join process by
 * traversing through the input streams (represented by `depth`) and their corresponding states. It
 * attempts to find matching combinations of rows across all inputs based on the defined join
 * conditions.
 *
 * <p><b>Core Idea:</b> The method explores a conceptual "join tree". Each level (`depth`)
 * corresponds to an input stream. At each level, it iterates through the records stored in the
 * state for that input. For each state record, it tentatively adds it to the `currentRows` array
 * and, if the relevant join condition passes ({@link #matchesCondition(int, RowData[])}),
 * recursively calls itself to process the next level (`depth + 1`). When the recursion reaches the
 * level corresponding to the triggering input record ({@link #isInputLevel(int, int)}), it
 * incorporates the `input` record itself into `currentRows` (again, subject to condition checks).
 * Finally, when the maximum depth is reached ({@link #isMaxDepth(int)}), it evaluates the final,
 * overall `multiJoinCondition` on the fully assembled `currentRows`.
 *
 * <p><b>Two-Phase Execution:</b> The recursion operates in two distinct phases, crucial for
 * correctly handling LEFT joins, managed by {@link #calculateAssociationsRecursive(int, RowData,
 * int, RowData[], int[])} and {@link #emitResultsRecursive(int, RowData, int, RowData[], int[])}:
 *
 * <ol>
 *   <li><b>Association Calculation (via {@code calculateAssociationsRecursive}):</b> This initial
 *       phase traverses the state. Its primary purpose is to calculate the `associations` counts
 *       for LEFT joins. This determines if rows from the "left" side found any matches on their
 *       respective "right" sides based on the {@link #joinConditions}. No results are emitted in
 *       this phase. When the recursion reaches the `inputId` level, it transitions to {@link
 *       #processInputRecordAndTransitionToEmit(int, RowData, int, RowData[], int[], boolean)}.
 *   <li><b>Result Emission (via {@code emitResultsRecursive}):</b> This phase is triggered by
 *       {@code processInputRecordAndTransitionToEmit} after the input record is processed. It
 *       incorporates the actual `input` record (or continues from state records in an emission
 *       context) and proceeds with the recursion. When the base case (checked via {@link
 *       #isMaxDepth(int)}) is reached, it evaluates the join conditions and emits the resulting
 *       joined row via the {@link #collector}.
 * </ol>
 *
 * <p><b>LEFT Join Specifics:</b> LEFT joins require special handling to ensure rows from the left
 * side are emitted even if they have no matching rows on the right side.
 *
 * <ul>
 *   <li><b>Condition Checks:</b>
 *       <ul>
 *         <li>At each step `d > 0`, the specific {@code joinConditions[d]} is evaluated using the
 *             rows accumulated so far (up to `currentRows[d]`). If this condition fails for a
 *             combination (from state or the input record), that recursive path is pruned via
 *             {@link #matchesCondition(int, RowData[])}.
 *         <li>At the maximum depth (base case), the final {@code multiJoinCondition} is evaluated
 *             on the complete `currentRows` array to determine if the overall joined row is valid.
 *       </ul>
 *   <li><b>Association Tracking ({@code associations} array):</b> {@code associations[d-1]} counts
 *       how many records from subsequent inputs (depth `d` onwards) have matched the current row at
 *       {@code currentRows[d-1]} based on the outer join conditions. This count is primarily
 *       updated during the {@code calculateAssociationsRecursive} phase, and also adjusted during
 *       the emission phase when processing the input record or state records that form part of an
 *       emitted join.
 *   <li><b>Null Padding:</b> If, after processing all state records for a LEFT join's right side
 *       (depth `d`) during either phase, no matches were found (`!matched`) AND the
 *       corresponding left row also had no associations ({@link #hasNoAssociations(int, int[])}),
 *       it indicates the left row needs to be padded with nulls for the right side. This triggers
 *       further recursion with a null row at `currentRows[d]` (e.g., within {@code
 *       calculateAssociationsRecursive} or {@code emitResultsRecursive}).
 *   <li><b>Input Record Handling (Upserts/Retractions via {@code
 *       processInputRecordAndTransitionToEmit}):</b> When processing the actual `input` record at
 *       its native depth (`inputId`) in a LEFT join scenario:
 *       <ul>
 *         <li>If the input is an INSERT/UPDATE_AFTER and its preceding left-side row had no matches
 *             found during the `calculateAssociationsRecursive` phase (checked via the {@code
 *             matchedInStateDuringCalcPhase} parameter), a retraction (`DELETE`) may be emitted
 *             first for any previously padded result.
 *         <li>If the input is a DELETE/UPDATE_BEFORE and its preceding left-side row now has no
 *             matches (checked via {@link #hasNoAssociations(int, int[])} after association
 *             updates), an insertion (`INSERT`) may be emitted for the new padded result.
 *       </ul>
 * </ul>
 *
 * <p><b>Base Case (Maximum Depth in {@code emitResultsRecursive}):</b> When {@link #isMaxDepth(int)}
 * is true during the emission phase, all potential contributing rows are in `currentRows`.
 *
 * <ul>
 *   <li>The final {@code multiJoinCondition} is evaluated on the complete `currentRows` array.
 *   <li>If the conditions pass, the combined row is constructed and emitted using {@link
 *       #emitRow(RowKind, RowData[])}.
 * </ul>
 *
 * <hr>
 *
 * <h3>Example Walkthrough (A LEFT JOIN B INNER JOIN C)</h3>
 *
 * <p>Inputs: A(idx=0), B(idx=1), C(idx=2)
 *
 * <p>Join: {@code A LEFT JOIN B ON A.id = B.id INNER JOIN C ON B.id = C.id}
 *
 * <p>Conditions:
 *
 * <ul>
 *   <li>{@code joinConditions[1]}: {@code A.id == B.id} (LEFT JOIN condition)
 *   <li>{@code joinConditions[2]}: {@code B.id == C.id} (INNER JOIN condition)
 *   <li>{@code multiJoinCondition}: {@code (A.id == B.id) && (B.id == C.id)} (Overall condition)
 * </ul>
 *
 * <p>Initial State:
 *
 * <ul>
 *   <li>StateA: {@code { a1(1, 100) }}
 *   <li>StateB: {@code { }}
 *   <li>StateC: {@code { c1(50, 501), c2(60, 601) }}
 * </ul>
 *
 * <p><b>=== Event 1: Input +b1(1, 50) arrives at Input B (inputId=1) ===</b>
 *
 * <pre><code>
 * Output: +I[a1(1,100), b1(1,50), c1(50,501)].
 * No INSERT for null padding emitted due to inner join with C. If this was
 * A LEFT JOIN B LEFT JOIN C instead of an inner join, we'd also retract this first -D[a1(1,100), NULL, NULL]).
 *
 * Initial Call: calculateAssociationsRecursive(0, +b1, 1, [_,_,_], [0,0,0])
 * // ... (Calculation phase similar to before, focusing on associations) ...
 * When depth reaches 1 (inputId):
 *   Call: processInputRecordAndTransitionToEmit(1, +b1, 1, [a1,_,_], [associations_from_calc], matched_state_calc_for_B)
 *     currentRows = [a1, +b1, _]
 *     (Handle retractions if needed based on matched_state_calc_for_B, associations - likely no retraction here)
 *     Call: emitResultsRecursive(2, +b1, 1, [a1,+b1,_], [updated_associations])
 *       Phase: EMIT_RESULTS
 *       Process StateC: { c1, c2 }
 *       Record c1: currentRows = [a1, +b1, c1]. matchesCondition(2, ...) -> true.
 *         Call: emitResultsRecursive(3, +b1, 1, [a1,+b1,c1], ...)
 *           isMaxDepth(3): true. multiJoinCondition(...) -> true.
 *           EMIT: +I[a1, b1, c1]
 *           Return true.
 *       Record c2: ... -> false.
 *       Return true (StateC loop matched).
 *     (Handle insertions if needed - likely no insertion here)
 *     Return from processInputRecordAndTransitionToEmit.
 * Return from calculateAssociationsRecursive.
 *
 * --- End Event 1 ---
 * Add record to StateB: +b1(1, 50) -> StateB becomes { b1(1, 50) }.
 * Output: +I[a1(1,100), b1(1,50), c1(50,501)].
 * </code></pre>
 *
 * <p><b>=== Event 2: Input delete -b1(1, 50) arrives at Input B (inputId=1) ===</b>
 *
 * <pre><code>
 * Before: StateB = { b1(1, 50) }
 * Output: -D[a1, b1, c1].
 * No INSERT for null padding emitted due to inner join with C.
 * If the query was A LEFT JOIN B LEFT JOIN C, we'd also emit a null padded row -I[a1(1,100), NULL, NULL].
 *
 * Initial Call: calculateAssociationsRecursive(0, -b1, 1, [_,_,_], [0,0,0])
 * // ... (Calculation phase, b1 is in StateB, associations for A with B are updated) ...
 * When depth reaches 1 (inputId):
 *   Call: processInputRecordAndTransitionToEmit(1, -b1, 1, [a1,_,_], [associations_from_calc], matched_state_calc_for_B)
 *     currentRows = [a1, -b1, _]
 *     (Handle retractions - no retraction, input is -b1)
 *     Call: emitResultsRecursive(2, -b1, 1, [a1,-b1,_], [updated_associations_after_processing_-b1])
 *       Phase: EMIT_RESULTS
 *       Process StateC: { c1, c2 }
 *       Record c1: currentRows = [a1, -b1, c1]. matchesCondition(2, ...) -> true.
 *         Call: emitResultsRecursive(3, -b1, 1, [a1,-b1,c1], ...)
 *           isMaxDepth(3): true. multiJoinCondition(...) -> true.
 *           EMIT: -D[a1, b1, c1] (original b1 data used due to RowKind.DELETE on input)
 *           Return true.
 *       Record c2: ... -> false.
 *       Return true (StateC loop matched).
 *     (Handle insertions: isRetraction(-b1) && isLeft && hasNoAssociations(...) -> true if C didn't match with null B)
 *       If so, call emitResultsRecursive(2, temp_+b1, 1, [a1,nullB,_], ...) -> leads to no emission if C requires non-null B.
 *     Return from processInputRecordAndTransitionToEmit.
 * Return from calculateAssociationsRecursive.
 *
 * --- End Event 2 ---
 * Add record to StateB: -b1(1, 50) -> StateB becomes {}.
 * Output: -D[a1, b1, c1].
 * </code></pre>
 */
public class StreamingMultiJoinOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData> {
    private static final long serialVersionUID = 1L;

    /** List of supported join types. */
    public enum JoinType {
        INNER,
        LEFT
    }

    private final List<JoinInputSideSpec> inputSpecs;
    private final List<JoinType> joinTypes;
    private final List<InternalTypeInfo<RowData>> inputTypes;
    // The multiJoinCondition is currently not being used, since we check the join conditions
    // for each while iterating through records to shortcircuit the recursion. However, if we
    // eventually want to cache join results at some level or do some other optimizations, this
    // might become useful.
    // TODO I'm not sure what's the best approach here: provide extra arguments so that we
    // avoid operator migrations or remove it since we don't need it for now.
    private final MultiJoinCondition multiJoinCondition;
    private final long[] stateRetentionTime;
    private final List<Input<RowData>> typedInputs;
    private final MultiJoinCondition[] joinConditions;
    private final JoinKeyExtractor keyExtractor;

    private transient List<MultiJoinStateView> stateHandlers;
    private transient TimestampedCollector<RowData> collector;
    private transient List<RowData> nullRows;

    public StreamingMultiJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            List<InternalTypeInfo<RowData>> inputTypes,
            List<JoinInputSideSpec> inputSpecs,
            List<JoinType> joinTypes,
            MultiJoinCondition multiJoinCondition,
            long[] stateRetentionTime,
            MultiJoinCondition[] joinConditions,
            JoinKeyExtractor keyExtractor) {
        super(parameters, inputSpecs.size());
        this.inputTypes = inputTypes;
        this.inputSpecs = inputSpecs;
        this.joinTypes = joinTypes;
        this.multiJoinCondition = multiJoinCondition;
        this.stateRetentionTime = stateRetentionTime;
        this.joinConditions = joinConditions;
        this.keyExtractor = keyExtractor;
        this.typedInputs = new ArrayList<>(inputSpecs.size());
    }

    @Override
    public void open() throws Exception {
        super.open();
        initializeCollector();
        initializeNullRows();
        initializeStateHandlers();
    }

    @Override
    public void close() throws Exception {
        closeConditions();
        super.close();
    }

    public void processElement(int inputId, StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        if (input == null) {
            return;
        }

        performMultiJoin(input, inputId);
        addRecordToState(input, inputId);
    }

    private void performMultiJoin(RowData input, int inputId) throws Exception {
        int[] associations = createInitialAssociations();
        RowData[] currentRows = new RowData[inputSpecs.size()];

        calculateAssociationsRecursive(0, input, inputId, currentRows, associations);
    }

    /**
     * Phase 1: Recursively processes state to calculate associations for LEFT joins.
     * It does not emit any results. When the recursion reaches the depth of the
     * triggering input record (`inputId`), it transitions to processing that input record,
     * which then leads to the emission phase.
     *
     * @return True if any match was found for currentRows[depth-1] with state at current depth,
     *         used for null padding logic within this phase.
     */
    private boolean calculateAssociationsRecursive(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            int[] associations)
            throws Exception {
        if (isMaxDepth(depth)) {
            // Base case for calculation phase: no emission, just return (no match at this empty path).
            return false;
        }

        boolean isLeft = isLeftJoinAtDepth(depth);
        boolean matchedAnyStateRecord = false;

        // Process state records for the current depth
        RowData joinKey = keyExtractor.getJoinKeyFromCurrentRows(depth, currentRows);
        Iterable<RowData> records = stateHandlers.get(depth).getRecords(joinKey);

        for (RowData record : records) {
            currentRows[depth] = record;

            if (matchesCondition(depth, currentRows)) {
                matchedAnyStateRecord = true;

                if (isLeft) {
                    associations[depth - 1]++; // Always increment for matches in calculation phase
                    // Reset associations for the current depth's right side before recursing
                    associations[depth] = 0;
                }

                // Optimization: If at input level, and this is a LEFT join,
                // we might be able to stop early if we only need to know if *any* match exists.
                // This is related to canOptimizeAssociationCounting.
                if (isLeft && depth == inputId && depth > 0) { // inputId is the current depth for its own state
                    // Apply canOptimizeAssociationCounting logic directly:
                    // For an upsert, if associations[depth-1] > 0, we are done for this path.
                    // For a retraction, if associations[depth-1] > 1, we are done.
                    // The `input` here is the main triggering input.
                    if (isUpsert(input) && associations[depth - 1] > 0) {
                        // Found at least one match for the left side (currentRows[depth-1])
                        // with a state record (record). If main input is upsert, this is enough.
                        // We must restore currentRows[depth] to null before returning, as the loop expects to set it.
                        currentRows[depth] = null; // Backtrack for this optimization path
                        return true; // Indicate a match was found for currentRows[depth-1]
                    } else if (isRetraction(input) && associations[depth - 1] > 1) {
                        // Main input is a retraction. If there was more than one match for the left side,
                        // removing one still leaves others. We are done for this path.
                        currentRows[depth] = null; // Backtrack
                        return true; // Indicate a match was found for currentRows[depth-1]
                    }
                }

                // If at the input level, we only calculate associations from its own state.
                // We do not recurse deeper *for these state records*.
                // The input record itself will be processed later by processInputRecordAndTransitionToEmit.
                if (isInputLevel(depth, inputId)) {
                    continue; // Next state record at this inputId level
                }

                // Recurse for calculation phase
                calculateAssociationsRecursive(depth + 1, input, inputId, currentRows, associations);
            } else {
                // Condition failed for this state record
                continue;
            }
        }
        currentRows[depth] = null; // Backtrack currentRows for this depth

        // After processing all state for the current depth:
        if (isInputLevel(depth, inputId)) {
            // Transition to processing the actual input record and then to emission phase.
            processInputRecordAndTransitionToEmit(depth, input, inputId, currentRows, associations, matchedAnyStateRecord);
        } else if (isLeft && !matchedAnyStateRecord) {
            // No state match for this left join's right side, and not at input level yet.
            // Process with null padding for the calculation phase.
            currentRows[depth] = nullRows.get(depth);
            calculateAssociationsRecursive(depth + 1, input, inputId, currentRows, associations);
            currentRows[depth] = null; // Backtrack null padding
        }
        return matchedAnyStateRecord;
    }

    /**
     * Handles the actual input record. It updates associations based on the input record,
     * manages retractions of prior null-padded rows if a new match is found,
     * initiates the emission phase for the input record, and handles insertions
     * of new null-padded rows if the input record was a retraction that removed the last match.
     */
    private void processInputRecordAndTransitionToEmit(
            int depth, // This will be equal to inputId
            RowData input,
            int inputId,
            RowData[] currentRows,
            int[] associations,
            boolean matchedInStateDuringCalcPhase) // Information from calculateAssociationsRecursive about current inputId level
            throws Exception {
        currentRows[depth] = input; // Place the actual input record into currentRows

        if (!matchesCondition(depth, currentRows)) {
            currentRows[depth] = null; // Backtrack
            return; // Input record itself doesn't satisfy condition at its level
        }

        boolean isLeft = isLeftJoinAtDepth(depth);

        // Update associations for the input record itself.
        // In the old system, this was: updateAssociationCount(depth, associations, shouldIncrementAssociation(false, input));
        // which simplifies to updateAssociationCount(depth, associations, isUpsert(input));
        if (isLeft) {
            updateAssociationCount(depth, associations, isUpsert(input));
        }

        // --- Left Join Retraction Handling (formerly part of processInputRecord) ---
        // If the input is an INSERT/UPDATE_AFTER on the right side of a LEFT join,
        // and its corresponding left-side row previously had no matches (indicated by !matchedInStateDuringCalcPhase),
        // the previous null-padded output must be retracted.
        if (isUpsert(input) && isLeft && !matchedInStateDuringCalcPhase) {
            // Temporarily place null to construct the row to be retracted
            RowData originalCurrentRowAtDepth = currentRows[depth]; // Should be the input
            currentRows[depth] = nullRows.get(depth);
            RowKind originalKind = input.getRowKind();
            input.setRowKind(RowKind.DELETE); // Temporarily change to DELETE for retraction

            emitResultsRecursive(depth + 1, input, inputId, currentRows, associations);

            input.setRowKind(originalKind); // Restore input's original RowKind
            currentRows[depth] = originalCurrentRowAtDepth; // Restore currentRows for this depth
        }

        // Continue recursion for the emission phase with the actual input record.
        // currentRows[depth] is already set to input.
        emitResultsRecursive(depth + 1, input, inputId, currentRows, associations);

        // --- Left Join Insertion Handling (formerly part of processInputRecord) ---
        // If an incoming DELETE/UPDATE_BEFORE on the right side of a LEFT join removes
        // the last matching record for the left-side row (checked via hasNoAssociations after updates),
        // a new null-padded result must be inserted.
        if (isRetraction(input) && isLeft && hasNoAssociations(depth, associations)) {
            // Temporarily place null to construct the row to be inserted
            RowData originalCurrentRowAtDepth = currentRows[depth]; // Should be the input
            currentRows[depth] = nullRows.get(depth);
            RowKind originalKind = input.getRowKind();
            input.setRowKind(RowKind.INSERT); // Temporarily change to INSERT for new padded row

            emitResultsRecursive(depth + 1, input, inputId, currentRows, associations);

            input.setRowKind(originalKind); // Restore input's original RowKind
            currentRows[depth] = originalCurrentRowAtDepth; // Restore currentRows for this depth
        }
        currentRows[depth] = null; // Backtrack currentRows for this depth after all processing
    }

    /**
     * Phase 2: Recursively processes state (and the initial input record path) to emit results.
     *
     * @return True if any match was found for currentRows[depth-1] with state at current depth,
     *         used for null padding logic within this phase.
     */
    private boolean emitResultsRecursive(
            int depth,
            RowData input, // The original triggering input
            int inputId,
            RowData[] currentRows,
            int[] associations)
            throws Exception {
        if (isMaxDepth(depth)) {
            // Base case for emission phase: emit the joined row.
            // The `input` parameter here is the original triggering input, used for its RowKind.
            emitJoinedRow(input, currentRows);
            return true; // Indicates a row was emitted (match found)
        }

        boolean isLeft = isLeftJoinAtDepth(depth);
        boolean matchedAnyStateRecord = false;

        // Process state records for the current depth during emission phase
        RowData joinKey = keyExtractor.getJoinKeyFromCurrentRows(depth, currentRows);
        Iterable<RowData> records = stateHandlers.get(depth).getRecords(joinKey);

        for (RowData record : records) {
            currentRows[depth] = record;

            if (matchesCondition(depth, currentRows)) {
                matchedAnyStateRecord = true;

                if (isLeft) {
                    // Update associations based on the state record being processed.
                    updateAssociationCount(depth, associations, isUpsert(record));
                    // Reset associations for the current depth's right side before recursing.
                    associations[depth] = 0;
                }

                // Recurse for emission phase
                emitResultsRecursive(depth + 1, input, inputId, currentRows, associations);
            } else {
                // Condition failed for this state record
                continue;
            }
        }
        currentRows[depth] = null; // Backtrack currentRows for this depth

        // After processing all state for the current depth during emission phase:
        // Handle null padding if this is a LEFT join and no state records matched.
        // This is not done if current depth is the inputId itself, as the input record path is handled by processInputRecordAndTransitionToEmit.
        if (isLeft && !matchedAnyStateRecord && !isInputLevel(depth, inputId)) {
            currentRows[depth] = nullRows.get(depth);
            emitResultsRecursive(depth + 1, input, inputId, currentRows, associations);
            currentRows[depth] = null; // Backtrack null padding
        }
        return matchedAnyStateRecord;
    }

    // Note: The original recursiveMultiJoin, processRecords, processWithNullPadding,
    // processInputRecord, and shouldIncrementAssociation methods will be removed
    // after their logic is migrated to the new methods.
    // The canOptimizeAssociationCounting method logic will be inlined or adapted
    // into calculateAssociationsRecursive.

    // This can simply emit the resulting join row between all n inputs.
    private void emitJoinedRow(RowData input, RowData[] currentRows) {
        emitRow(input.getRowKind(), currentRows);
    }

    private void addRecordToState(RowData input, int inputId) throws Exception {
        RowData joinKey = keyExtractor.getJoinKeyFromInput(input, inputId);

        if (isRetraction(input)) {
            stateHandlers.get(inputId).retractRecord(joinKey, input);
        } else {
            stateHandlers.get(inputId).addRecord(joinKey, input);
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

    private void initializeStateHandlers() {
        if (this.stateHandler.getKeyedStateStore().isPresent()) {
            getRuntimeContext().setKeyedStateStore(this.stateHandler.getKeyedStateStore().get());
        } else {
            throw new RuntimeException(
                    "Keyed state store not found when initializing keyed state store handlers.");
        }

        this.stateHandlers = new ArrayList<>(inputSpecs.size());
        for (int i = 0; i < inputSpecs.size(); i++) {
            MultiJoinStateView stateView;
            String stateName = "multi-join-input-" + i;
            InternalTypeInfo<RowData> joinKeyType = keyExtractor.getJoinKeyType(i);

            if (joinKeyType == null) {
                throw new IllegalStateException(
                        "Could not determine joinKeyType for input "
                                + i
                                + ". State requires identifiable key attributes derived from join conditions.");
            }

            stateView =
                    MultiJoinStateViews.create(
                            getRuntimeContext(),
                            stateName,
                            inputSpecs.get(i),
                            joinKeyType,
                            inputTypes.get(i),
                            stateRetentionTime[i]);
            stateHandlers.add(stateView);
            typedInputs.add(createInput(i + 1));
        }
    }

    private void closeConditions() throws Exception {
        if (multiJoinCondition != null) {
            multiJoinCondition.close();
        }
    }

    private Input<RowData> createInput(int idx) {
        return new AbstractInput<>(this, idx) {
            @Override
            public void processElement(StreamRecord<RowData> element) throws Exception {
                ((StreamingMultiJoinOperator) owner)
                        .processElement(
                                idx - 1, // All internal logic is 0-based, so adjust the input ID.
                                element);
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
        return depth > 0 && joinTypes.get(depth) == JoinType.LEFT;
    }

    /** Checks if the join condition specific to the current depth holds true. */
    private boolean matchesCondition(int depth, RowData[] currentRows) {
        // The first input (depth 0) doesn't have a preceding input to join with,
        // so there's no specific join condition to evaluate against `joinConditions[0]`.
        return depth == 0 || joinConditions[depth].apply(currentRows);
    }

    private void updateAssociationCount(int depth, int[] associations, boolean isUpsert) {
        if (isUpsert) {
            associations[depth - 1]++;
        } else {
            associations[depth - 1]--;
        }
    }

    private int[] createInitialAssociations() {
        int[] associations = new int[inputSpecs.size()];
        Arrays.fill(associations, 0);
        return associations;
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    public List<Input> getInputs() {
        // Instead of a direct cast from List<Input<RowData>> to List<Input> (which fails due to
        // Java's generics type erasure and invariance for collections),
        // we must create a new List<Input> and add elements. This is safe because
        // Input<RowData> is a subtype of the raw Input type.
        @SuppressWarnings({"rawtypes"})
        List<Input> rawInputs = new ArrayList<>(typedInputs.size());
        rawInputs.addAll(typedInputs);
        return rawInputs;
    }

    private boolean isMaxDepth(int depth) {
        return depth == inputSpecs.size();
    }

    private boolean isInputLevel(int depth, int inputId) {
        return depth == inputId;
    }

    private boolean hasNoAssociations(int depth, int[] associations) {
        return depth > 0 && associations[depth - 1] == 0;
    }

    /**
     * Optimization for LEFT joins during the CALCULATE_MATCHES phase at the input level.
     *
     * <p>We only need to know if *any* match exists (for upserts) or if *more than one* match
     * exists (for retractions) to correctly handle null padding logic later in the EMIT_RESULTS
     * phase. Counting beyond this minimum requirement during CALCULATE_MATCHES is unnecessary.
     *
     * <p>Note: If further optimizations involving caching exact association counts are added, this
     * optimization might need to be removed.
     */
    private boolean canOptimizeAssociationCounting(
            int depth, int inputId, RowData input, int[] associations) {
        // This optimization is only relevant at the specific depth of the current input record
        // and not for the initial input (depth 0, which has no preceding associations).
        if (depth == 0 || inputId != depth) {
            return false;
        }

        if (isUpsert(input)) {
            // For an upsert, if at least one match is found (associations > 0),
            // we know a retraction of a prior null-padded row (if any) won't be necessary
            // before emitting the new joined row. Further counting adds no value here.
            return associations[depth - 1] > 0;
        } else {
            // For a retraction, if more than one match existed (associations > 1),
            // removing this one input record means other matches still exist.
            // Therefore, we won't need to insert a new null-padded row after this retraction.
            // If associations[depth-1] was 1, then after decrementing it becomes 0,
            // and we would need to insert a null padded row.
            return associations[depth - 1] > 1;
        }
    }
}
