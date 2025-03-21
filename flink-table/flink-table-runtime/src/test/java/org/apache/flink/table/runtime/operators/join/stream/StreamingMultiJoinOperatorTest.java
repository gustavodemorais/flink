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
        // Add a user record with key "1"
        insertUser("1", "Gus", "User 1 Details");

        // No output yet since we haven't received matching record from second input
        assertor.shouldEmitNothing(testHarness);

        // Add an order with matching key "1"
        insertOrder("1", "order_1", "Order 1 Details");

        // Should emit joined record since keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details",
                        "1", "order_1", "Order 1 Details"));

        // Add an order with non-matching key "2"
        insertOrder("2", "order_2", "Order 2 Details");

        // Should not emit since keys don't match
        assertor.shouldEmitNothing(testHarness);

        // Add matching user record for key "2"
        insertUser("2", "Bob", "User 2 Details");

        // Should emit joined record for key "2"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "2", "Bob", "User 2 Details",
                        "2", "order_2", "Order 2 Details"));
    }

    @TestTemplate
    void testTwoWayInnerJoinUpdating() throws Exception {
        // Setup initial data
        insertUser("1", "Gus", "User 1 Details");
        
        // No output yet since we haven't received matching record from second input
        assertor.shouldEmitNothing(testHarness);

        insertOrder("1", "order_1", "Order 1 Details");
        
        // Should emit joined record since keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details",
                        "1", "order_1", "Order 1 Details"));

        // Update user details
        updateAfterUser("1", "Gus", "User 1 Details Updated");

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1", "Gus", "User 1 Details Updated",
                        "1", "order_1", "Order 1 Details"));

        // Update order details
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1", "Gus", "User 1 Details Updated",
                        "1", "order_1", "Order 1 Details Updated"));

        // Delete the order record for key 1, which should generate a deletion for the join
        deleteOrder("1", "order_1", "Order 1 Details Updated");

        // Should emit a delete for the old join result
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1", "Gus", "User 1 Details Updated",
                        "1", "order_1", "Order 1 Details Updated"));

        // Add a matching order record back for key "1"
        insertOrder("1", "order_1", "Order 1 New Details");

        // Should emit joined record for key "1"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details Updated",
                        "1", "order_1", "Order 1 New Details"));
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
        // For outer join test, set outerJoinFlags to true for all inputs to test full outer join
        super(2, List.of(JoinRelType.INNER, JoinRelType.LEFT), false);
    }

    @TestTemplate
    void testTwoWayLeftOuterJoin() throws Exception {
        // Add a user record with key "1"
        insertUser("1", "Gus", "User 1 Details");

        // Should emit joined record with null values for right side (for left outer join)
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details",
                        null, null, null));

        // Add an order record with key "2" that has no match in first input
        insertOrder("2", "order_2", "Order 2 Details");

        // Should not emit anything for right-only record since this is a LEFT join
        assertor.shouldEmitNothing(testHarness);

        // Add an order record with matching key "1"
        insertOrder("1", "order_1", "Order 1 Details");

        // Should emit an update to the previous left outer join result
        // First delete the old record with nulls and then emit the joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.DELETE, "1", "Gus", "User 1 Details", null, null, null),
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details",
                        "1", "order_1", "Order 1 Details"));

        // Delete order 1
        deleteOrder("1", "order_1", "Order 1 Details");

        // Should revert to left outer join result with nulls for the right side
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1", "Gus", "User 1 Details",
                        "1", "order_1", "Order 1 Details"),
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details",
                        null, null, null));

        // Add order back
        insertOrder("1", "order_1", "Order 1 Details");

        // Join output should be restored
        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.DELETE, "1", "Gus", "User 1 Details", null, null, null),
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details",
                        "1", "order_1", "Order 1 Details"));

        // Delete user 1
        deleteUser("1", "Gus", "User 1 Details");

        // Should delete the join result since there's no left record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1", "Gus", "User 1 Details",
                        "1", "order_1", "Order 1 Details"));

        // Add user back
        insertUser("1", "Gus", "User 1 Details");

        // Join output should be restored
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details",
                        "1", "order_1", "Order 1 Details"));

        // Update user with before record
        updateBeforeUser("1", "Gus", "User 1 Details");

        // Should emit before update
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "1", "Gus", "User 1 Details",
                        "1", "order_1", "Order 1 Details"));

        // Update user with after record
        updateAfterUser("1", "Gus", "User 1 Details Updated");
        
        // Should emit after update
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1", "Gus", "User 1 Details Updated",
                        "1", "order_1", "Order 1 Details"));

        // Update user again with only after record
        updateAfterUser("1", "Gus", "User 1 Details Updated 2");
        
        // Should emit after update
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1", "Gus", "User 1 Details Updated 2",
                        "1", "order_1", "Order 1 Details"));

        // Update order with before record
        updateBeforeOrder("1", "order_1", "Order 1 Details");

        // Should emit before update and insert null placeholders
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "1", "Gus", "User 1 Details Updated 2",
                        "1", "order_1", "Order 1 Details"),
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details Updated 2",
                        null, null, null));

        // Update order with after record
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");
        
        // Should delete null placeholders and emit update
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1", "Gus", "User 1 Details Updated 2",
                        null, null, null),
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1", "Gus", "User 1 Details Updated 2",
                        "1", "order_1", "Order 1 Details Updated"));

        // Update order again with only after record
        updateAfterOrder("1", "order_1", "Order 1 Details Updated 2");
        
        // Should emit after update
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1", "Gus", "User 1 Details Updated 2",
                        "1", "order_1", "Order 1 Details Updated 2"));

        // Add a second order for the same user
        insertOrder("1", "order_2", "Order 2 Details");

        // Should emit additional join result
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details Updated 2",
                        "1", "order_2", "Order 2 Details"));

        // Delete user who has multiple matching orders
        deleteUser("1", "Gus", "User 1 Details Updated 2");

        // Should emit deletes for all join results
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1", "Gus", "User 1 Details Updated 2",
                        "1", "order_1", "Order 1 Details Updated 2"),
                rowOfKind(
                        RowKind.DELETE,
                        "1", "Gus", "User 1 Details Updated 2",
                        "1", "order_2", "Order 2 Details"));

        // Add a new user with same key but different name
        insertUser("1", "Charlie", "User 3 Details");

        // Should emit join results for both orders
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Charlie", "User 3 Details",
                        "1", "order_1", "Order 1 Details Updated 2"),
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Charlie", "User 3 Details",
                        "1", "order_2", "Order 2 Details"));
    }
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
        // Add a user record with key "1"
        insertUser("1", "Gus", "User 1 Details");

        // No output yet since we haven't received matching records from other inputs
        assertor.shouldEmitNothing(testHarness);

        // Add an order with matching key "1"
        insertOrder("1", "order_1", "Order 1 Details");

        // Still no output - need all three inputs to match
        assertor.shouldEmitNothing(testHarness);

        // Add a payment with matching key "1"
        insertPayment("1", "payment_1", "Payment 1 Details");

        // Should emit joined record since all three keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details",
                        "1", "order_1", "Order 1 Details",
                        "1", "payment_1", "Payment 1 Details"));

        // Test with another set of records with key "2"
        insertUser("2", "Bob", "User 2 Details");
        insertOrder("2", "order_2", "Order 2 Details");

        // No output yet - need all three inputs to match
        assertor.shouldEmitNothing(testHarness);

        insertPayment("2", "payment_2", "Payment 2 Details");

        // Should emit joined record for key "2"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "2", "Bob", "User 2 Details",
                        "2", "order_2", "Order 2 Details",
                        "2", "payment_2", "Payment 2 Details"));
    }

    @TestTemplate
    void testThreeWayInnerJoinUpdating() throws Exception {
        // Set up three-way join
        insertUser("1", "Gus", "User 1 Details");
        insertOrder("1", "order_1", "Order 1 Details");
        insertPayment("1", "payment_1", "Payment 1 Details");

        // Should emit joined record since all three keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details",
                        "1", "order_1", "Order 1 Details",
                        "1", "payment_1", "Payment 1 Details"));

        // Update user details
        updateAfterUser("1", "Gus", "User 1 Details Updated");

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1", "Gus", "User 1 Details Updated",
                        "1", "order_1", "Order 1 Details",
                        "1", "payment_1", "Payment 1 Details"));

        // Update order details
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1", "Gus", "User 1 Details Updated",
                        "1", "order_1", "Order 1 Details Updated",
                        "1", "payment_1", "Payment 1 Details"));

        // Update payment details
        updateAfterPayment("1", "payment_1", "Payment 1 Details Updated");

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "1", "Gus", "User 1 Details Updated",
                        "1", "order_1", "Order 1 Details Updated",
                        "1", "payment_1", "Payment 1 Details Updated"));

        // Delete the payment record for key 1, which should generate a deletion for the join
        deletePayment("1", "payment_1", "Payment 1 Details Updated");

        // Should emit a delete for the old join result
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "1", "Gus", "User 1 Details Updated",
                        "1", "order_1", "Order 1 Details Updated",
                        "1", "payment_1", "Payment 1 Details Updated"));

        // Add a matching payment record back for key "1"
        insertPayment("1", "payment_1", "Payment 1 New Details");

        // Should emit joined record for key "1"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "1", "Gus", "User 1 Details Updated",
                        "1", "order_1", "Order 1 Details Updated",
                        "1", "payment_1", "Payment 1 New Details"));

        // Test key updates by inserting records with key "2"
        insertUser("2", "Bob", "User 2 Details");
        insertOrder("2", "order_2", "Order 2 Details");
        insertPayment("2", "payment_2", "Payment 2 Details");

        // Should emit joined record for key "2"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "2", "Bob", "User 2 Details",
                        "2", "order_2", "Order 2 Details",
                        "2", "payment_2", "Payment 2 Details"));

        // Delete user 2
        deleteUser("2", "Bob", "User 2 Details");

        // Should emit a delete for the old join result
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "2", "Bob", "User 2 Details",
                        "2", "order_2", "Order 2 Details",
                        "2", "payment_2", "Payment 2 Details"));

        // Add updated matching record for key "2"
        insertUser("2", "Bob_Updated", "User 2 Details Updated");

        // Should emit the row again with updated records
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "2", "Bob_Updated", "User 2 Details Updated",
                        "2", "order_2", "Order 2 Details",
                        "2", "payment_2", "Payment 2 Details"));
    }
}
