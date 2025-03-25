package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedMultiJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.runtime.util.StreamRecordUtils;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// Add constants for RowKind values to improve readability

public abstract class StreamingMultiJoinOperatorTestBase {

    protected final List<InternalTypeInfo<RowData>> inputTypeInfos;
    protected final List<RowDataKeySelector> keySelectors;
    protected final List<JoinInputSideSpec> inputSpecs;
    protected final List<JoinRelType> joinTypes;
    protected final List<GeneratedMultiJoinCondition> outerJoinConditions;
    protected final boolean isFullOuterJoin;

    protected final InternalTypeInfo<RowData> joinKeyTypeInfo;
    protected RowDataHarnessAssertor assertor;
    protected KeyedMultiInputStreamOperatorTestHarness<String, RowData> testHarness;

    protected List<KeySelector<RowData, String>> dummyKeySelectors;

    // Define commonly used RowKind constants to make the test code more readable
    protected static final RowKind INSERT = RowKind.INSERT;
    protected static final RowKind UPDATE_BEFORE = RowKind.UPDATE_BEFORE;
    protected static final RowKind UPDATE_AFTER = RowKind.UPDATE_AFTER;
    protected static final RowKind DELETE = RowKind.DELETE;
    protected static final List<GeneratedMultiJoinCondition> DEFAULT_CONDITIONS = new ArrayList<>(3);

    protected StreamingMultiJoinOperatorTestBase(
            int numInputs, List<JoinRelType> joinTypes, List<GeneratedMultiJoinCondition> outerJoinConditions,  boolean isFullOuterJoin) {
        // Initialize collections
        this.inputTypeInfos = new ArrayList<>(numInputs);
        this.keySelectors = new ArrayList<>(numInputs);
        this.inputSpecs = new ArrayList<>(numInputs);
        this.joinTypes = joinTypes;
        this.isFullOuterJoin = isFullOuterJoin;
        this.outerJoinConditions = outerJoinConditions;

        // Initialize default types for each input
        for (int i = 0; i < numInputs; i++) {
            inputTypeInfos.add(createInputTypeInfo(i));
            keySelectors.add(createKeySelector(i));
        }

        for (int i = 0; i < numInputs; i++) {
            // Create input specs
            inputSpecs.add(
                    JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                            inputTypeInfos.get(i), keySelectors.get(i)));
        }

        if (outerJoinConditions.isEmpty()) {
            for (int i = 0; i < numInputs; i++) {
                if (this.joinTypes.get(i) != JoinRelType.INNER) {
                    outerJoinConditions.add(createMultiJoinOuterJoinCondition(i, i - 1));
                } else {
                    outerJoinConditions.add(null);
                }
            }
        }

        // Join key type (assuming common key type across all inputs)
        this.joinKeyTypeInfo = InternalTypeInfo.of(new CharType(false, 20));

        // TODO Gustavo get rid of this
        this.dummyKeySelectors = keySelectorsDummy();
    }

    // Assertion helper methods

    protected static List<GeneratedMultiJoinCondition> defaultConditions() {
        return new ArrayList<>();
    }

    /** Assert that one row is emitted with the given kind and field values. */
    protected void emits(RowKind kind, String... fields) throws Exception {
        assertor.shouldEmit(testHarness, rowOfKind(kind, fields));
    }

    /** Assert that no rows are emitted. */
    protected void emitsNothing() throws Exception {
        assertor.shouldEmitNothing(testHarness);
    }

    /** Assert that two rows are emitted with the given kinds and field values. */
    protected void emits(RowKind kind1, String[] fields1, RowKind kind2, String[] fields2)
            throws Exception {
        assertor.shouldEmit(testHarness, rowOfKind(kind1, fields1), rowOfKind(kind2, fields2));
    }

    protected void emits(
            RowKind kind1,
            String[] fields1,
            RowKind kind2,
            String[] fields2,
            RowKind kind3,
            String[] fields3)
            throws Exception {
        assertor.shouldEmit(
                testHarness,
                rowOfKind(kind1, fields1),
                rowOfKind(kind2, fields2),
                rowOfKind(kind3, fields3));
    }

    protected void emits(
            RowKind kind1,
            String[] fields1,
            RowKind kind2,
            String[] fields2,
            RowKind kind3,
            String[] fields3,
            RowKind kind4,
            String[] fields4)
            throws Exception {
        assertor.shouldEmit(
                testHarness,
                rowOfKind(kind1, fields1),
                rowOfKind(kind2, fields2),
                rowOfKind(kind3, fields3),
                rowOfKind(kind4, fields4));
    }

    /** Helper method to create a StreamRecord with the given kind and fields. */
    protected RowData rowOfKind(RowKind kind, String... fields) {
        return StreamRecordUtils.rowOfKind(kind, fields);
    }

    // Helper methods for inserting records
    protected void insertUser(String userId, String userName, String details) throws Exception {
        testHarness.processElement(0, StreamRecordUtils.insertRecord(userId, userName, details));
    }

    protected void insertOrder(String userId, String orderId, String details) throws Exception {
        testHarness.processElement(1, StreamRecordUtils.insertRecord(userId, orderId, details));
    }

    protected void insertPayment(String userId, String paymentId, String details) throws Exception {
        testHarness.processElement(2, StreamRecordUtils.insertRecord(userId, paymentId, details));
    }

    // Helper methods for updating records - with before AND after
    protected void updateBeforeUser(String userId, String userName, String details)
            throws Exception {
        testHarness.processElement(
                0, StreamRecordUtils.updateBeforeRecord(userId, userName, details));
    }

    protected void updateAfterUser(String userId, String userName, String details)
            throws Exception {
        testHarness.processElement(
                0, StreamRecordUtils.updateAfterRecord(userId, userName, details));
    }

    protected void updateBeforeOrder(String userId, String orderId, String details)
            throws Exception {
        testHarness.processElement(
                1, StreamRecordUtils.updateBeforeRecord(userId, orderId, details));
    }

    protected void updateAfterOrder(String userId, String orderId, String details)
            throws Exception {
        testHarness.processElement(
                1, StreamRecordUtils.updateAfterRecord(userId, orderId, details));
    }

    protected void updateBeforePayment(String userId, String paymentId, String details)
            throws Exception {
        testHarness.processElement(
                2, StreamRecordUtils.updateBeforeRecord(userId, paymentId, details));
    }

    protected void updateAfterPayment(String userId, String paymentId, String details)
            throws Exception {
        testHarness.processElement(
                2, StreamRecordUtils.updateAfterRecord(userId, paymentId, details));
    }

    // Helper methods for deleting records
    protected void deleteUser(String userId, String userName, String details) throws Exception {
        testHarness.processElement(0, StreamRecordUtils.deleteRecord(userId, userName, details));
    }

    protected void deleteOrder(String userId, String orderId, String details) throws Exception {
        testHarness.processElement(1, StreamRecordUtils.deleteRecord(userId, orderId, details));
    }

    protected void deletePayment(String userId, String paymentId, String details) throws Exception {
        testHarness.processElement(2, StreamRecordUtils.deleteRecord(userId, paymentId, details));
    }

    // Generic helper methods for dynamic input index
    protected void insertRecord(int inputIndex, String... fields) throws Exception {
        testHarness.processElement(inputIndex, StreamRecordUtils.insertRecord(fields));
    }

    protected void updateBeforeRecord(int inputIndex, String... fields) throws Exception {
        testHarness.processElement(inputIndex, StreamRecordUtils.updateBeforeRecord(fields));
    }

    protected void updateAfterRecord(int inputIndex, String... fields) throws Exception {
        testHarness.processElement(inputIndex, StreamRecordUtils.updateAfterRecord(fields));
    }

    protected void deleteRecord(int inputIndex, String... fields) throws Exception {
        testHarness.processElement(inputIndex, StreamRecordUtils.deleteRecord(fields));
    }

    /**
     * Helper method to create a collection of field values for multi-row assertions. This improves
     * readability when using the emits() method with multiple rows.
     */
    protected String[] r(String... values) {
        return values;
    }

    protected InternalTypeInfo<RowData> createInputTypeInfo(int inputIndex) {
        return InternalTypeInfo.of(
                RowType.of(
                        new LogicalType[] {
                            new CharType(false, 20),
                            new CharType(false, 20),
                            VarCharType.STRING_TYPE
                        },
                        new String[] {
                            String.format("user_id_%d", inputIndex),
                            String.format("id_%d", inputIndex),
                            String.format("details_%d", inputIndex)
                        }));
    }

    // TODO Gustavo not used yet but will be soon for the test base the 0 has to be replaced by the
    // correct index 0 or 1 for each table
    protected RowDataKeySelector createKeySelector(int inputIndex) {
        return HandwrittenSelectorUtil.getRowDataSelector(
                new int[] {0}, // Assuming key is always second column
                inputTypeInfos
                        .get(inputIndex)
                        .toRowType()
                        .getChildren()
                        .toArray(new LogicalType[0]));
    }

    @BeforeEach
    void beforeEach() throws Exception {
        testHarness = createTestHarness();
        setupKeySelectorsForTestHarness(testHarness);
        testHarness.setup();
        testHarness.open();
        assertor =
                new RowDataHarnessAssertor(
                        getOutputType().getChildren().toArray(new LogicalType[0]));
    }

    @AfterEach
    void afterEach() throws Exception {
        if (testHarness != null) {
            testHarness.close();
        }
    }

    /** Get the output row type of join operator. */
    protected RowType getOutputType() {
        var typesStream =
                this.inputTypeInfos.stream()
                        .flatMap(typeInfo -> typeInfo.toRowType().getChildren().stream());
        var namesStream =
                this.inputTypeInfos.stream()
                        .flatMap(typeInfo -> typeInfo.toRowType().getFieldNames().stream());

        return RowType.of(
                typesStream.toArray(LogicalType[]::new), namesStream.toArray(String[]::new));
    }

    /** Factory class for creating StreamingMultiWayJoinOperator instances. */
    private static class MultiStreamingJoinOperatorFactory
            extends AbstractStreamOperatorFactory<RowData> {

        private final List<JoinInputSideSpec> inputSpecs;
        private final List<KeySelector<RowData, String>> dummyKeySelectors;
        protected final List<InternalTypeInfo<RowData>> inputTypeInfos;
        private final List<JoinRelType> joinTypes;
        private final List<GeneratedMultiJoinCondition> outerJoinConditions;
        private final boolean isFullOuterJoin;

        public MultiStreamingJoinOperatorFactory(
                List<JoinInputSideSpec> inputSpecs,
                List<KeySelector<RowData, String>> dummyKeySelectors,
                List<InternalTypeInfo<RowData>> inputTypeInfos,
                List<JoinRelType> joinTypes,
                List<GeneratedMultiJoinCondition> outerJoinConditions,
                boolean isFullOuterJoin) {
            this.inputSpecs = inputSpecs;
            this.dummyKeySelectors = dummyKeySelectors;
            this.inputTypeInfos = inputTypeInfos;
            this.joinTypes = joinTypes;
            this.outerJoinConditions = outerJoinConditions;
            this.isFullOuterJoin = isFullOuterJoin;
        }

        @Override
        public <T extends StreamOperator<RowData>> T createStreamOperator(
                StreamOperatorParameters<RowData> parameters) {
            StreamingMultiJoinOperator op =
                    createJoinOperator(
                            parameters, inputSpecs, dummyKeySelectors, inputTypeInfos, joinTypes, outerJoinConditions);
            return (T) op;
        }

        @Override
        public Class<? extends StreamOperator<RowData>> getStreamOperatorClass(
                ClassLoader classLoader) {
            return StreamingMultiJoinOperator.class;
        }

        protected StreamingMultiJoinOperator createJoinOperator(
                StreamOperatorParameters<RowData> parameters,
                List<JoinInputSideSpec> inputSpecs,
                List<KeySelector<RowData, String>> dummyKeySelectors,
                List<InternalTypeInfo<RowData>> inputTypeInfos,
                List<JoinRelType> joinTypes,
                List<GeneratedMultiJoinCondition> outerJoinConditions) {
            // Create join conditions (for now just using a simple condition that always returns
            // true)
            List<GeneratedJoinCondition> generatedConditions = new ArrayList<>();
            for (int i = 0; i < inputSpecs.size(); i++) {
                generatedConditions.add(createJoinCondition());
            }

            // Convert generated conditions to runtime conditions
            List<JoinCondition> joinConditions = new ArrayList<>();
            for (GeneratedJoinCondition genCond : generatedConditions) {
                try {
                    joinConditions.add(genCond.newInstance(getClass().getClassLoader()));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to instantiate join condition", e);
                }
            }

            // Create filter nulls array (for now setting all to false)
            boolean[] filterNulls = new boolean[inputSpecs.size()];
            Arrays.fill(filterNulls, false);

            // Create state retention time array
            long[] stateRetentionTime = new long[inputSpecs.size()];
            Arrays.fill(stateRetentionTime, 0L);

            // Create multi-join condition if we're using multiple inputs
            MultiJoinCondition multiJoinCondition = null;
            if (inputSpecs.size() > 1) {
                try {
                    multiJoinCondition =
                            createMultiJoinCondition(inputSpecs.size())
                                    .newInstance(getClass().getClassLoader());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to instantiate multi-join condition", e);
                }
            }

            // array filled with max rentention time
            long[] retentionTime = new long[inputSpecs.size()];
            for (int i = 0; i < inputSpecs.size(); i++) {
                retentionTime[i] = 9999999L;
            }

            // Create outer join conditions array based on outerJoinFlags
            MultiJoinCondition[] outJoinConditions;
            outJoinConditions = new MultiJoinCondition[inputSpecs.size()];
            for (int i = 0; i < inputSpecs.size(); i++) {
                if (joinTypes.get(i) != JoinRelType.INNER) {
                    // For inputs marked as outer join, instantiate a condition
                    try {
                        outJoinConditions[i] =
                                outerJoinConditions.get(i)
                                        .newInstance(getClass().getClassLoader());
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to instantiate outer join condition, make sure you're passing an outer condition for every outer join", e);
                    }
                } else {
                    // For regular inputs, use null
                    outJoinConditions[i] = null;
                }
            }

            return new StreamingMultiJoinOperator(
                    parameters,
                    inputTypeInfos,
                    inputSpecs,
                    dummyKeySelectors,
                    joinTypes,
                    joinConditions,
                    multiJoinCondition,
                    filterNulls,
                    retentionTime,
                    isFullOuterJoin,
                    outJoinConditions);
        }

        private GeneratedJoinCondition createJoinCondition() {
            String funcCode =
                    "public class ConditionFunction extends org.apache.flink.api.common.functions.AbstractRichFunction "
                            + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                            + "\n"
                            + "    public ConditionFunction(Object[] reference) {\n"
                            + "    }\n"
                            + "\n"
                            + "    @Override\n"
                            + "    public boolean apply(org.apache.flink.table.data.RowData in1, org.apache.flink.table.data.RowData in2) {\n"
                            + "        // Compare the key fields (second column, index 1)\n"
                            + "        if (in1.isNullAt(0) || in2.isNullAt(0)) {\n"
                            + "            return false;\n"
                            + "        }\n"
                            + "        return in1.getString(0).toString().equals(in2.getString(0).toString());\n"
                            + "    }\n"
                            + "\n"
                            + "    @Override\n"
                            + "    public void close() throws Exception {\n"
                            + "        super.close();\n"
                            + "    }\n"
                            + "}\n";
            return new GeneratedJoinCondition("ConditionFunction", funcCode, new Object[0]);
        }

        // Create a dummy MultiJoinCondition that checks if all inputs have the same join key value
        private GeneratedMultiJoinCondition createMultiJoinCondition(int numInputs) {
            String funcCode =
                    "public class MultiConditionFunction extends org.apache.flink.api.common.functions.AbstractRichFunction "
                            + "implements org.apache.flink.table.runtime.generated.MultiJoinCondition {\n"
                            + "\n"
                            + "    public MultiConditionFunction(Object[] reference) {\n"
                            + "    }\n"
                            + "\n"
                            + "    @Override\n"
                            + "    public boolean apply(org.apache.flink.table.data.RowData[] inputs) {\n"
                            + "        // If any input doesn't exist yet, we can't evaluate\n"
                            + "        if (inputs == null || inputs.length < "
                            + numInputs
                            + ") {\n"
                            + "            return false;\n"
                            + "        }\n"
                            + "\n"
                            + "        // First, check for nulls\n"
                            + "        for (org.apache.flink.table.data.RowData input : inputs) {\n"
                            + "            if (input == null || input.isNullAt(0)) {\n"
                            + "                return false;\n"
                            + "            }\n"
                            + "        }\n"
                            + "\n"
                            + "        // Get the first key as reference\n"
                            + "        String referenceKey = inputs[0].getString(0).toString();\n"
                            + "\n"
                            + "        // Check if all keys match the reference\n"
                            + "        for (int i = 1; i < inputs.length; i++) {\n"
                            + "            String currentKey = inputs[i].getString(0).toString();\n"
                            + "            if (!referenceKey.equals(currentKey)) {\n"
                            + "                return false;\n"
                            + "            }\n"
                            + "        }\n"
                            + "\n"
                            + "        return true;\n"
                            + "    }\n"
                            + "\n"
                            + "    @Override\n"
                            + "    public void close() throws Exception {\n"
                            + "        super.close();\n"
                            + "    }\n"
                            + "}\n";
            return new org.apache.flink.table.runtime.generated.GeneratedMultiJoinCondition(
                    "MultiConditionFunction", funcCode, new Object[0]);
        }
    }

    private void setupKeySelectorsForTestHarness(
            KeyedMultiInputStreamOperatorTestHarness<String, RowData> harness) {
        for (int i = 0; i < this.inputSpecs.size(); i++) {
            // TODO Gustavo The key selector for the state has to be always 0
            // Because we want to all arrows associated with the id 0

            // this is used to partition state, figure out how to do it properly
            // we need one per input? hm idk
            KeySelector<RowData, String> keySelector = row -> row.getString(0).toString();
            harness.setKeySelector(i, keySelector);
        }
    }

    private List<KeySelector<RowData, String>> keySelectorsDummy() {
        List<KeySelector<RowData, String>> hardKeySelectors = new ArrayList<>();
        for (int i = 0; i < this.inputSpecs.size(); i++) {
            // TODO gustavo hard coded key - 0 for the orders table and 1 for the other ones
            var keyIndex = i == 0 ? 0 : 1;
            KeySelector<RowData, String> keySelector = row -> row.getString(keyIndex).toString();
            hardKeySelectors.add(keySelector);
        }
        return hardKeySelectors;
    }

    protected KeyedMultiInputStreamOperatorTestHarness<String, RowData> createTestHarness()
            throws Exception {
        return new KeyedMultiInputStreamOperatorTestHarness<>(
                new MultiStreamingJoinOperatorFactory(
                        inputSpecs, dummyKeySelectors, inputTypeInfos, joinTypes, outerJoinConditions, isFullOuterJoin),
                TypeInformation.of(String.class));
    }

    protected static GeneratedMultiJoinCondition createMultiJoinOuterJoinCondition(int index, int indexToCompare) {
        String funcCode =
                "public class MultiOuterJoinConditionFunction extends org.apache.flink.api.common.functions.AbstractRichFunction "
                        + "implements org.apache.flink.table.runtime.generated.MultiJoinCondition {\n"
                        + "\n"
                        + "    private final int index;\n"
                        + "\n"
                        + "    public MultiOuterJoinConditionFunction(Object[] reference) {\n"
                        + "        this.index = "
                        + index
                        + ";\n"
                        + "    }\n"
                        + "\n"
                        + "    @Override\n"
                        + "    public boolean apply(org.apache.flink.table.data.RowData[] inputs) {\n"
                        + "        // Check if we have both inputs to compare\n"
                        + "        if (inputs == null || index < 1 || inputs["+indexToCompare+"] == null || inputs[index] == null) {\n"
                        + "            return false;\n"
                        + "        }\n"
                        + "\n"
                        + "        // Check for nulls in key columns\n"
                        + "        if (inputs["+indexToCompare+" ].isNullAt(0) || inputs[index].isNullAt(0)) {\n"
                        + "            return false;\n"
                        + "        }\n"
                        + "\n"
                        + "        // Compare only input[indexToCompare] with inputs[index]\n"
                        + "        String firstKey = inputs["+indexToCompare+"].getString(0).toString();\n"
                        + "        String secondKey = inputs[index].getString(0).toString();\n"
                        + "        return firstKey.equals(secondKey);\n"
                        + "    }\n"
                        + "\n"
                        + "    @Override\n"
                        + "    public void close() throws Exception {\n"
                        + "        super.close();\n"
                        + "    }\n"
                        + "}\n";
        return new GeneratedMultiJoinCondition(
                "MultiOuterJoinConditionFunction", funcCode, new Object[0]);
    }
}
