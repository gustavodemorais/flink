package org.apache.flink.table.runtime.operators.join.stream;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.*;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

@ExtendWith(ParameterizedTestExtension.class)
class StreamingTwoWayInnerMultiJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    @Parameters(name = "enableAsyncState = {0}")
    public static List<Boolean> enableAsyncState() {
        return Arrays.asList(false);
    }

    @Parameter private boolean enableAsyncState;

    public StreamingTwoWayInnerMultiJoinOperatorTest() {
        // For inner join test, set outerJoinFlags to false for all inputs
        super(2, List.of(JoinRelType.INNER, JoinRelType.INNER), false);
    }

    @TestTemplate
    void testTwoWayInnerJoin() throws Exception {
        // Process first input - add a record with key "1"
        testHarness.processElement(
                0,
                insertRecord(
                        "1", // user_id
                        "Gus", // user_name
                        "User 1 Details" // details
                        ));

        // No output yet since we haven't received matching record from second input
        assertor.shouldEmitNothing(testHarness);

        // Process second input - add a record with matching key "1"
        testHarness.processElement(
                1,
                insertRecord(
                        "1", // user_id
                        "order_1", // order_id
                        "Order 1 Details" // details
                        ));

        // Should emit joined record since keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details"));

        // Process second input with non-matching key
        testHarness.processElement(1, insertRecord("2", "order_2", "Order 2 Details"));

        // Should not emit since keys don't match
        assertor.shouldEmitNothing(testHarness);

        // Add matching record to first input
        testHarness.processElement(0, insertRecord("2", "Bob", "User 2 Details"));

        // Should emit joined record for key "2"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details"));
    }

    @TestTemplate
    void testTwoWayInnerJoinUpdating() throws Exception {
        // Process first input - add a record with key "1"
        testHarness.processElement(0, insertRecord("1", "Gus", "User 1 Details"));

        // No output yet since we haven't received matching record from second input
        assertor.shouldEmitNothing(testHarness);

        // Process second input - add a record with matching key "1"
        testHarness.processElement(1, insertRecord("1", "order_1", "Order 1 Details"));

        // Should emit joined record since keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details"));

        // Update first input record
        testHarness.processElement(0, updateAfterRecord("1", "Gus", "User 1 Details Updated"));

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details"));

        // Update second input record
        testHarness.processElement(1, updateAfterRecord("1", "order_1", "Order 1 Details Updated"));

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated"));

        // Delete the shipment record for key 1, which should generate a deletion for the join
        testHarness.processElement(1, deleteRecord("1", "order_1", "Order 1 Details Updated"));

        // Should emit a delete for the old join result
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated"));

        // Add a matching shipment record back for key "1"
        testHarness.processElement(1, insertRecord("1", "order_1", "Order 1 New Details"));

        // Should emit joined record for key "1"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 New Details"));
    }
}

@ExtendWith(ParameterizedTestExtension.class)
class StreamingTwoWayOuterMultiJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    @Parameters(name = "enableAsyncState = {0}")
    public static List<Boolean> enableAsyncState() {
        return Arrays.asList(false);
    }

    @Parameter private boolean enableAsyncState;

    public StreamingTwoWayOuterMultiJoinOperatorTest() {
        // todo gustavo double chheck how to set the array properly here
        // For outer join test, set outerJoinFlags to true for all inputs to test full outer join
        super(2, List.of(JoinRelType.INNER, JoinRelType.LEFT), false);
    }

    @TestTemplate
    void testTwoWayLeftOuterJoin() throws Exception {
        // Process first input - add a record with key "1"
        /* -------------------------------------  +I APPEND ------------------------------------- */
        testHarness.processElement(
                0,
                insertRecord(
                        "1", // user_id
                        "Gus", // user_name
                        "User 1 Details" // details
                ));

        // Should emit joined record with null values for right side (for left outer join)
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details",
                        null, // Right side user_id is null
                        null, // Right side order_id is null
                        null  // Right side details is null
                ));

        // Process second input - add a record with key "2" that has no match in first input
        testHarness.processElement(
                1,
                insertRecord(
                        "2", // user_id
                        "order_2", // order_id
                        "Order 2 Details" // details
                ));

        // Should emit joined record with null values for left side (for right outer join)
        assertor.shouldEmitNothing(testHarness);

        // Process second input - add a record with matching key "1"
        testHarness.processElement(
                1,
                insertRecord(
                        "1", // user_id
                        "order_1", // order_id
                        "Order 1 Details" // details
                ));

        // Should emit an update to the previous left outer join result
        // First delete the old record with nulls and then
        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.DELETE, "1", "Gus", "User 1 Details", null, null, null),
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details"));

        /* -------------------------------------  -D DELETE ------------------------------------- */

        // DELETE order 1
        testHarness.processElement(
                1,
                deleteRecord(
                        "1", // user_id
                        "order_1", // order_id
                        "Order 1 Details" // details
                ));

        // Should emit an update to the previous left outer join result
        // First delete the old record with nulls and then
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details"),
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details",
                        null,
                        null,
                        null));

        // Add value back
        testHarness.processElement(
                1,
                insertRecord(
                        "1", // user_id
                        "order_1", // order_id
                        "Order 1 Details" // details
                ));

        // Join output should be there
        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.DELETE, "1", "Gus", "User 1 Details", null, null, null),
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details"));

        // DELETE USER 1
        testHarness.processElement(
                0,
                deleteRecord(
                        "1", // user_id
                        "Gus", // user_name
                        "User 1 Details" // details
                ));

        // Should emit an update to the previous left outer join result
        // First delete the old record with nulls and then
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details"));

        // Add value back
        testHarness.processElement(
                0,
                insertRecord(
                        "1", // user_id
                        "Gus", // user_name
                        "User 1 Details" // details
                ));

        // Join output should be there
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details"));

        /* -----------------------------------  -U UPDATE BEFORE ---------------------------------*/

        // ----- UPDATE USERS
        testHarness.processElement(0, updateBeforeRecord("1", "Gus", "User 1 Details"));

        // First with -U and +U
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details"));

        testHarness.processElement(0, updateAfterRecord("1", "Gus", "User 1 Details Updated"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details"));

        testHarness.processElement(0, updateAfterRecord("1", "Gus", "User 1 Details Updated 2"));
        // Update only with +u
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1",
                        "Gus",
                        "User 1 Details Updated 2",
                        "1",
                        "order_1",
                        "Order 1 Details"));

        // ----- UPDATE ORDERS
        // TODO question check RowKind.INSERT
        // Update first input record
        testHarness.processElement(1, updateBeforeRecord("1", "order_1", "Order 1 Details"));

        // First with -U and +U
        // TODO Gustavo should we emit a +I(order_1,1,Order 1 Updated,null,null,null)] - check
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "1",
                        "Gus",
                        "User 1 Details Updated 2",
                        "1",
                        "order_1",
                        "Order 1 Details"),
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details Updated 2",
                        null,
                        null,
                        null));

        testHarness.processElement(1, updateAfterRecord("1", "order_1", "Order 1 Details Updated"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1",
                        "Gus",
                        "User 1 Details Updated 2",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1",
                        "Gus",
                        "User 1 Details Updated 2",
                        "1",
                        "order_1",
                        "Order 1 Details Updated"));

        testHarness.processElement(1, updateAfterRecord("1", "order_1", "Order 1 Details Updated 2"));

        // Update only with +u
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1",
                        "Gus",
                        "User 1 Details Updated 2",
                        "1",
                        "order_1",
                        "Order 1 Details Updated 2"));

        /* -------------------------------  +I +I MULTI APPEND --------------------------------- */

        // +I ORDER - second order for id 1
        testHarness.processElement(
                1,
                insertRecord(
                        "1", // user_id
                        "order_2", // order_id
                        "Order 2 Details" // details
                ));

        // Insert for new order
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details Updated 2",
                        "1",
                        "order_2",
                        "Order 2 Details"));

        /* -------------------------------------  -D DELETE ------------------------------------- */

        // Delete first input record with key "1"
        testHarness.processElement(0, deleteRecord("1", "Gus", "User 1 Details Updated 2"));

        // Should emit a delete for the join result and
        // a right outer join record for order_2 since user was deleted
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1",
                        "Gus",
                        "User 1 Details Updated 2",
                        "1",
                        "order_1",
                        "Order 1 Details Updated 2"),
                rowOfKind(
                        RowKind.DELETE,
                        "1",
                        "Gus",
                        "User 1 Details Updated 2",
                        "1",
                        "order_2",
                        "Order 2 Details"));

        // Add a matching user record back for key "1"
        testHarness.processElement(0, insertRecord("1", "Charlie", "User 3 Details"));

        // Should delete the right outer join record and a joined record for key "1"
        // TODO Gustavo does this order makes sense?
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Charlie",
                        "User 3 Details",
                        "1",
                        "order_1",
                        "Order 1 Details Updated 2"),
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Charlie",
                        "User 3 Details",
                        "1",
                        "order_2",
                        "Order 2 Details"));
    }

    // TODO Gustavo Look into emiting an update before for unique update after so we drop (optimization)
    // TODO Gustavo partial deletes: join conditions has only unique key, other fields are null and the output is the same (we get the old value from state) - ( optimization 2)

}

@ExtendWith(ParameterizedTestExtension.class)
class StreamingThreeWayJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    @Parameters(name = "enableAsyncState = {0}")
    public static List<Boolean> enableAsyncState() {
        return Arrays.asList(false);
    }

    @Parameter private boolean enableAsyncState;

    public StreamingThreeWayJoinOperatorTest() {
        // For inner join test, set outerJoinFlags to false for all inputs
        super(3, List.of(JoinRelType.INNER, JoinRelType.INNER, JoinRelType.INNER), false);
    }

    @TestTemplate
    void testThreeWayInnerJoin() throws Exception {
        // Process first input - add a record with key "1"
        testHarness.processElement(0, insertRecord("1", "Gus", "User 1 Details"));

        // No output yet since we haven't received matching records from other inputs
        assertor.shouldEmitNothing(testHarness);

        // Process second input - add a record with matching key "1"
        testHarness.processElement(1, insertRecord("1", "order_1", "Order 1 Details"));

        // Still no output - need all three inputs to match
        assertor.shouldEmitNothing(testHarness);

        // Process third input - add a record with matching key "1"
        testHarness.processElement(2, insertRecord("1", "payment_1", "Payment 1 Details"));

        // Should emit joined record since all three keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        "1",
                        "payment_1",
                        "Payment 1 Details"));

        // Test non-matching keys
        testHarness.processElement(0, insertRecord("2", "Bob", "User 2 Details"));

        testHarness.processElement(1, insertRecord("2", "order_2", "Order 2 Details"));

        // No output yet - need all three to match
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement(2, insertRecord("2", "payment_2", "Payment 2 Details"));

        // Should emit joined record for key "2"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_2",
                        "Payment 2 Details"));
    }

    @TestTemplate
    void testThreeWayInnerJoinUpdating() throws Exception {
        // Process first input - add a record with key "1"
        testHarness.processElement(0, insertRecord("1", "Gus", "User 1 Details"));

        // No output yet since we haven't received matching records from other inputs
        assertor.shouldEmitNothing(testHarness);

        // Process second input - add a record with matching key "1"
        testHarness.processElement(1, insertRecord("1", "order_1", "Order 1 Details"));

        // Still no output - need all three inputs to match
        assertor.shouldEmitNothing(testHarness);

        // Process third input - add a record with matching key "1"
        testHarness.processElement(2, insertRecord("1", "payment_1", "Payment 1 Details"));

        // Should emit joined record since all three keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        "1",
                        "payment_1",
                        "Payment 1 Details"));

        // Update first input record
        testHarness.processElement(0, updateAfterRecord("1", "Gus", "User 1 Details Updated"));

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        "1",
                        "payment_1",
                        "Payment 1 Details"));

        // Update second input record - only update the details field
        testHarness.processElement(1, updateAfterRecord("1", "order_1", "Order 1 Details Updated"));

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_1",
                        "Payment 1 Details"));

        // Update third input record - only update the details field
        testHarness.processElement(2, updateAfterRecord("1", "payment_1", "Payment 1 Details Updated"));

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_1",
                        "Payment 1 Details Updated"));

        // Delete the payment record for key 1, which should generate a deletion for the join
        testHarness.processElement(2, deleteRecord("1", "payment_1", "Payment 1 Details Updated"));

        // Should emit a delete for the old join result
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_1",
                        "Payment 1 Details Updated"));

        // Add a matching payment record back for key "1"
        testHarness.processElement(2, insertRecord("1", "payment_1", "Payment 1 New Details"));

        // Should emit joined record for key "1"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_1",
                        "Payment 1 New Details"));

        // Test key updates by inserting records with key "2"
        testHarness.processElement(0, insertRecord("2", "Bob", "User 2 Details"));

        testHarness.processElement(1, insertRecord("2", "order_2", "Order 2 Details"));

        testHarness.processElement(2, insertRecord("2", "payment_2", "Payment 2 Details"));

        // Should emit joined record for key "2"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_2",
                        "Payment 2 Details"));

        // Update key of order_2 from "2" to "3"
        testHarness.processElement(0, deleteRecord("2", "Bob", "User 2 Details"));

        // Should emit a delete for the old join result
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_2",
                        "Payment 2 Details"));

        // Add updated matching record for key "2"
        testHarness.processElement(0, insertRecord("2", "Bob_Updated", "User 2 Details Updated"));

        // Should emit the row again with updated records
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "2",
                        "Bob_Updated",
                        "User 2 Details Updated",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_2",
                        "Payment 2 Details"));
    }
}
