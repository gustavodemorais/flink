package org.apache.flink.table.runtime.operators.join.stream;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.*;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.types.RowKind.UPDATE_BEFORE;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
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

    /**
     * SELECT u.*, o.*
     * FROM Users u
     * INNER JOIN Orders o ON u.id = o.user_id
     */
    @TestTemplate
    void testTwoWayInnerJoin() throws Exception {
        /* -------- APPEND TESTS ----------- */
        
        // Users without orders aren't emitted
        insertUser("1", "Gus", "User 1 Details");
        emitsNothing();

        // User joins with matching order
        insertOrder("1", "order_1", "Order 1 Details");
        emits(INSERT, "1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details");

        // Orders without users aren't emitted
        insertOrder("2", "order_2", "Order 2 Details");
        emitsNothing();

        // Adding matching user triggers join
        insertUser("2", "Bob", "User 2 Details");
        emits(INSERT, "2", "Bob", "User 2 Details", "2", "order_2", "Order 2 Details");
    }

    /**
     * SELECT u.*, o.*
     * FROM Users u
     * INNER JOIN Orders o ON u.id = o.user_id
     * -- Test updates and deletes on both sides
     */
    @TestTemplate
    void testTwoWayInnerJoinUpdating() throws Exception {
        /* -------- SETUP BASE DATA ----------- */
        insertUser("1", "Gus", "User 1 Details");
        emitsNothing();
        
        insertOrder("1", "order_1", "Order 1 Details");
        emits(INSERT, "1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details");

        /* -------- UPDATE TESTS ----------- */
        
        // +U on user.details emits +U
        updateAfterUser("1", "Gus", "User 1 Details Updated");
        emits(UPDATE_AFTER, "1", "Gus", "User 1 Details Updated", "1", "order_1", "Order 1 Details");

        // +U on order.details emits +U
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");
        emits(UPDATE_AFTER, "1", "Gus", "User 1 Details Updated", "1", "order_1", "Order 1 Details Updated");

        /* -------- DELETE TESTS ----------- */
        
        // -D on order emits -D
        deleteOrder("1", "order_1", "Order 1 Details Updated");
        emits(DELETE, "1", "Gus", "User 1 Details Updated", "1", "order_1", "Order 1 Details Updated");

        // Re-insert order emits +I
        insertOrder("1", "order_1", "Order 1 New Details");
        emits(INSERT, "1", "Gus", "User 1 Details Updated", "1", "order_1", "Order 1 New Details");
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

    /**
     * SELECT u.*, o.*
     * FROM Users u
     * LEFT OUTER JOIN Orders o ON u.id = o.user_id
     * -- Test left outer join behavior with nulls and transitions
     */
    @TestTemplate
    void testTwoWayLeftOuterJoin() throws Exception {
        /* -------- LEFT OUTER JOIN APPEND TESTS ----------- */
        
        // Left table row always emits, even without matching right row
        insertUser("1", "Gus", "User 1 Details");
        emits(INSERT, "1", "Gus", "User 1 Details", null, null, null);

        // Right-only record not emitted (LEFT join)
        insertOrder("2", "order_2", "Order 2 Details");
        emitsNothing();

        /* -------- MATCH/UNMATCH TRANSITIONS ----------- */
        
        // Add matching order - deletes null result, emits joined
        insertOrder("1", "order_1", "Order 1 Details");
        emits(
                DELETE, r("1", "Gus", "User 1 Details", null, null, null),
                INSERT, r("1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details"));

        // Delete order - reverts to left outer join result
        deleteOrder("1", "order_1", "Order 1 Details");
        emits(
                DELETE, r("1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details"),
                INSERT, r("1", "Gus", "User 1 Details", null, null, null));

        // Re-add order - transitions back to inner join
        insertOrder("1", "order_1", "Order 1 Details");
        emits(
                DELETE, r("1", "Gus", "User 1 Details", null, null, null),
                INSERT, r("1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details"));

        /* -------- USER DELETE/REINSERT TESTS ----------- */
        
        // Delete left record removes entire result
        deleteUser("1", "Gus", "User 1 Details");
        emits(DELETE, "1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details");

        // Re-add user restores join
        insertUser("1", "Gus", "User 1 Details");
        emits(INSERT, "1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details");

        /* -------- USER UPDATE TESTS ----------- */
        
        // -U on user emits -U
        updateBeforeUser("1", "Gus", "User 1 Details");
        emits(UPDATE_BEFORE, "1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details");

        // +U on user emits +U
        updateAfterUser("1", "Gus", "User 1 Details Updated");
        emits(UPDATE_AFTER, "1", "Gus", "User 1 Details Updated", "1", "order_1", "Order 1 Details");

        // Another +U on user
        updateAfterUser("1", "Gus", "User 1 Details Updated 2");
        emits(UPDATE_AFTER, "1", "Gus", "User 1 Details Updated 2", "1", "order_1", "Order 1 Details");

        /* -------- ORDER UPDATE TESTS ----------- */
        
        // -U on order emits -U and temporarily reverts to left outer
        updateBeforeOrder("1", "order_1", "Order 1 Details");
        emits(
                UPDATE_BEFORE, r("1", "Gus", "User 1 Details Updated 2", "1", "order_1", "Order 1 Details"),
                INSERT, r("1", "Gus", "User 1 Details Updated 2", null, null, null));

        // +U on order removes null result and emits join
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");
        emits(
                DELETE, r("1", "Gus", "User 1 Details Updated 2", null, null, null),
                UPDATE_AFTER, r("1", "Gus", "User 1 Details Updated 2", "1", "order_1", "Order 1 Details Updated"));

        // Another +U on order
        updateAfterOrder("1", "order_1", "Order 1 Details Updated 2");
        emits(UPDATE_AFTER, "1", "Gus", "User 1 Details Updated 2", "1", "order_1", "Order 1 Details Updated 2");

        /* -------- MULTI-ROW TESTS ----------- */
        
        // Adding second order for same user
        insertOrder("1", "order_2", "Order 2 Details");
        emits(INSERT, "1", "Gus", "User 1 Details Updated 2", "1", "order_2", "Order 2 Details");

        // Delete user with multiple orders deletes all join results
        deleteUser("1", "Gus", "User 1 Details Updated 2");
        emits(
                DELETE, r("1", "Gus", "User 1 Details Updated 2", "1", "order_1", "Order 1 Details Updated 2"),
                DELETE, r("1", "Gus", "User 1 Details Updated 2", "1", "order_2", "Order 2 Details"));

        // New user with same key joins with both orders
        insertUser("1", "Dawid", "User 3 Details");
        emits(
                INSERT, r("1", "Dawid", "User 3 Details", "1", "order_1", "Order 1 Details Updated 2"),
                INSERT, r("1", "Dawid", "User 3 Details", "1", "order_2", "Order 2 Details"));
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

    /**
     * SELECT u.*, o.*, p.*
     * FROM Users u
     * INNER JOIN Orders o ON u.id = o.user_id
     * INNER JOIN Payments p ON u.id = p.user_id
     * -- Test three-way inner join with append-only data
     */
    @TestTemplate
    void testThreeWayInnerJoin() throws Exception {
        /* -------- THREE-WAY JOIN APPEND TESTS ----------- */
        
        // First table alone doesn't emit
        insertUser("1", "Gus", "User 1 Details");
        emitsNothing();

        // First two tables don't emit
        insertOrder("1", "order_1", "Order 1 Details");
        emitsNothing();

        // All three tables match emits join
        insertPayment("1", "payment_1", "Payment 1 Details");
        emits(INSERT, "1", "Gus", "User 1 Details", 
                     "1", "order_1", "Order 1 Details",
                     "1", "payment_1", "Payment 1 Details");

        // Testing with second set of records
        insertUser("2", "Bob", "User 2 Details");
        insertOrder("2", "order_2", "Order 2 Details");
        emitsNothing();

        insertPayment("2", "payment_2", "Payment 2 Details");
        emits(INSERT, "2", "Bob", "User 2 Details",
                     "2", "order_2", "Order 2 Details",
                     "2", "payment_2", "Payment 2 Details");
    }

    /**
     * SELECT u.*, o.*, p.*
     * FROM Users u
     * INNER JOIN Orders o ON u.id = o.user_id
     * INNER JOIN Payments p ON u.id = p.user_id
     * -- Test updates and deletes across all three tables
     */
    @TestTemplate
    void testThreeWayInnerJoinUpdating() throws Exception {
        /* -------- SETUP BASE DATA ----------- */
        
        // Set up initial three-way join
        insertUser("1", "Gus", "User 1 Details");
        insertOrder("1", "order_1", "Order 1 Details");
        insertPayment("1", "payment_1", "Payment 1 Details");
        emits(INSERT, "1", "Gus", "User 1 Details",
                     "1", "order_1", "Order 1 Details",
                     "1", "payment_1", "Payment 1 Details");

        /* -------- UPDATE TESTS ----------- */
        
        // +U on user emits +U
        updateAfterUser("1", "Gus", "User 1 Details Updated");
        emits(UPDATE_AFTER, "1", "Gus", "User 1 Details Updated",
                          "1", "order_1", "Order 1 Details",
                          "1", "payment_1", "Payment 1 Details");

        // +U on order emits +U
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");
        emits(UPDATE_AFTER, "1", "Gus", "User 1 Details Updated",
                          "1", "order_1", "Order 1 Details Updated",
                          "1", "payment_1", "Payment 1 Details");

        // +U on payment emits +U
        updateAfterPayment("1", "payment_1", "Payment 1 Details Updated");
        emits(UPDATE_AFTER, "1", "Gus", "User 1 Details Updated",
                          "1", "order_1", "Order 1 Details Updated",
                          "1", "payment_1", "Payment 1 Details Updated");

        /* -------- DELETE/REINSERT TESTS ----------- */
        
        // -D on payment emits -D for join
        deletePayment("1", "payment_1", "Payment 1 Details Updated");
        emits(DELETE, "1", "Gus", "User 1 Details Updated",
                    "1", "order_1", "Order 1 Details Updated",
                    "1", "payment_1", "Payment 1 Details Updated");

        // Re-add payment emits +I
        insertPayment("1", "payment_1", "Payment 1 New Details");
        emits(INSERT, "1", "Gus", "User 1 Details Updated",
                     "1", "order_1", "Order 1 Details Updated",
                     "1", "payment_1", "Payment 1 New Details");

        /* -------- SECOND JOIN TESTS ----------- */
        
        // Adding a second set with key "2"
        insertUser("2", "Bob", "User 2 Details");
        insertOrder("2", "order_2", "Order 2 Details");
        insertPayment("2", "payment_2", "Payment 2 Details");
        emits(INSERT, "2", "Bob", "User 2 Details",
                     "2", "order_2", "Order 2 Details",
                     "2", "payment_2", "Payment 2 Details");

        // Delete user 2 emits -D
        deleteUser("2", "Bob", "User 2 Details");
        emits(DELETE, "2", "Bob", "User 2 Details",
                    "2", "order_2", "Order 2 Details",
                    "2", "payment_2", "Payment 2 Details");

        // Re-add user 2 with update emits +I
        insertUser("2", "Bob_Updated", "User 2 Details Updated");
        emits(INSERT, "2", "Bob_Updated", "User 2 Details Updated",
                     "2", "order_2", "Order 2 Details",
                     "2", "payment_2", "Payment 2 Details");
    }
}
