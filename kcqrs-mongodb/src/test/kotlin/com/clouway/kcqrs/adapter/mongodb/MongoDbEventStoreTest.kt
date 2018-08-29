package com.clouway.kcqrs.adapter.mongodb

import com.clouway.kcqrs.core.*
import com.clouway.kcqrs.testing.TestMessageFormat
import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import org.hamcrest.CoreMatchers
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.hamcrest.CoreMatchers.`is` as Is
import org.junit.Assert.assertThat
import java.util.*

/**
 * @author Tsvetozar Bonev (tsbonev@gmail.com)
 */
class MongoDbEventStoreTest {

    private lateinit var client: MongoClient
    private lateinit var server: MongoServer
    private lateinit var aggregateBase: MongoDbEventStore

    @Before
    fun setUp() {
        server = MongoServer(MemoryBackend())
        val serverAddress = server.bind()
        client = MongoClient(ServerAddress(serverAddress))

        aggregateBase = MongoDbEventStore("db",
                "Events",
                TestMessageFormat(),
                client,
                true,
                1000000)
    }

    @After
    fun tearDown() {
        client.close()
        server.shutdown()
    }

    @Test
    fun getEventsThatAreStored() {
        val result = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")))
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents(result.aggregateId)

        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, Is(CoreMatchers.equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        result.aggregateId,
                                        "Invoice",
                                        null,
                                        1,
                                        listOf(
                                                EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))
                                        )
                                ))
                        )
                        ))))

            }
            else -> Assert.fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun multipleEvents() {
        val result = aggregateBase.saveEvents("Order", listOf(
                EventPayload("::kind1::", 1L, "::user 1::", Binary("event1-data")),
                EventPayload("::kind2::", 2L, "::user 2::", Binary("event2-data"))
        )) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents(result.aggregateId)

        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, Is(CoreMatchers.equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        result.aggregateId,
                                        "Order",
                                        null,
                                        2,
                                        listOf(
                                                EventPayload("::kind1::", 1L, "::user 1::", Binary("event1-data")),
                                                EventPayload("::kind2::", 2L, "::user 2::", Binary("event2-data"))
                                        )
                                ))

                        ))
                )))
            }
            else -> Assert.fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun getMultipleAggregates() {
        val result1 = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")))
        ) as SaveEventsResponse.Success

        val result2 = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")))
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents(listOf(result1.aggregateId, result2.aggregateId))

        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response.aggregates, Is(CoreMatchers.hasItems(
                        Aggregate(
                                result1.aggregateId,
                                "Invoice",
                                null,
                                1,
                                listOf(
                                        EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))
                                )
                        ),
                        Aggregate(
                                result2.aggregateId,
                                "Invoice",
                                null,
                                1,
                                listOf(
                                        EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))
                                )
                        ))))

            }
            else -> Assert.fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun getMultipleAggregatesButNoneMatched() {
        val response = aggregateBase.getEvents(listOf("::id 1::", "::id 2::"))

        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, Is(CoreMatchers.equalTo((
                        GetEventsResponse.Success(
                                listOf())
                        ))))

            }
            else -> Assert.fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun newAggregateIdIsIssuedIfItsNotProvided() {
        val result = aggregateBase.saveEvents("A1", listOf(EventPayload("::kind1::", 1L, "::user id1::", Binary("aggregate1-event1-data")))) as SaveEventsResponse.Success
        val result2 = aggregateBase.saveEvents("A1", listOf(EventPayload("::kind::", 2L, "::user id2::", Binary("aggregate2-event1-data")))) as SaveEventsResponse.Success

        assertThat(result.aggregateId, Is(CoreMatchers.not(CoreMatchers.equalTo(result2.aggregateId))))
    }

    @Test
    fun detectEventCollisions() {
        val aggregateId = UUID.randomUUID().toString()

        aggregateBase.saveEvents("Order", listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))), SaveOptions(aggregateId = aggregateId, version = 0))

        val saveResult = aggregateBase.saveEvents("Order", listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))), SaveOptions(aggregateId = aggregateId, version = 0))

        when (saveResult) {
            is SaveEventsResponse.EventCollision -> {
                assertThat(saveResult.aggregateId, Is(CoreMatchers.equalTo(aggregateId)))
                assertThat(saveResult.expectedVersion, Is(CoreMatchers.equalTo(1L)))
            }
            else -> Assert.fail("got un-expected save result: $saveResult")
        }
    }

    @Test
    fun revertSavedEvents() {
        val aggregateId = UUID.randomUUID().toString()

        aggregateBase.saveEvents("Task", listOf(
                EventPayload("::kind 1::", 1L, "::user1::", Binary("data0")),
                EventPayload("::kind 2::", 2L, "::user1::", Binary("data1")),
                EventPayload("::kind 3::", 3L, "::user1::", Binary("data2")),
                EventPayload("::kind 4::", 4L, "::user2::", Binary("data3")),
                EventPayload("::kind 5::", 5L, "::user2::", Binary("data4"))
        ), SaveOptions(aggregateId = aggregateId, version = 0))

        val response = aggregateBase.revertLastEvents(aggregateId, 2)
        when (response) {
            is RevertEventsResponse.Success -> {

                val resp = aggregateBase.getEvents(aggregateId) as GetEventsResponse.Success
                assertThat(resp, Is(CoreMatchers.equalTo(
                        GetEventsResponse.Success(listOf(Aggregate(aggregateId,
                                "Task",
                                null,
                                3,
                                listOf(
                                        EventPayload("::kind 1::", 1L, "::user1::", Binary("data0")),
                                        EventPayload("::kind 2::", 2L, "::user1::", Binary("data1")),
                                        EventPayload("::kind 3::", 3L, "::user1::", Binary("data2"))
                                ))))
                )))
            }
            else -> Assert.fail("got un-expected response '$response' when reverting saved events")
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun revertZeroEventsIsNotAllowed() {
        aggregateBase.revertLastEvents("::any id::", 0)
    }

    @Test
    fun revertingMoreThenTheAvailableEvents() {
        val aggregateId = UUID.randomUUID().toString()

        aggregateBase.saveEvents("A1", listOf(
                EventPayload("::kind 1::", 1L, "::user id::", Binary("data0")),
                EventPayload("::kind 2::", 2L, "::user id::", Binary("data1"))
        ), SaveOptions(aggregateId = aggregateId, version = 0))

        val response = aggregateBase.revertLastEvents(aggregateId, 5)
        when (response) {
            is RevertEventsResponse.ErrorNotEnoughEventsToRevert -> {
            }
            else -> Assert.fail("got un-expected response '$response' when reverting more then available")
        }
    }

    @Test
    fun revertFromUnknownAggregate() {
        aggregateBase.revertLastEvents("::unknown aggregate::", 1) as RevertEventsResponse.AggregateNotFound
    }

    @Test
    fun saveStringWithLargeSize() {
        val tooBigStringData = "aaaaa".repeat(100000) // 500,170 bytes in BSON form
        val result = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary(tooBigStringData)))
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents(result.aggregateId)

        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, Is(CoreMatchers.equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        result.aggregateId,
                                        "Invoice",
                                        null,
                                        1,
                                        listOf(
                                                EventPayload("::kind::", 1L, "::user 1::", Binary(tooBigStringData))
                                        )
                                ))
                        )
                        ))))

            }
            else -> Assert.fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun saveEventExceedsEntityLimitationsAndReturnsCurrentEvents() {
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::")
        )

        val tooBigStringData = "aaaaaaaa".repeat(150000)

        val eventLimitReachedResponse = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary(tooBigStringData))),
                SaveOptions("::aggregateId::", 1)
        ) as SaveEventsResponse.SnapshotRequired

        assertThat(eventLimitReachedResponse.currentEvents, Is(CoreMatchers.equalTo(listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))))))
    }

    @Test
    fun returningManyEventsOnLimitReached() {
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::", 0)
        )

        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))),
                SaveOptions("::aggregateId::", 1)
        )

        val tooBigStringData = "aaaaaaaa".repeat(150000)

        val eventLimitReachedResponse = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary(tooBigStringData))),
                SaveOptions("::aggregateId::", 2)
        ) as SaveEventsResponse.SnapshotRequired

        assertThat(eventLimitReachedResponse.currentEvents, Is(CoreMatchers.equalTo(listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")), EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))))))
    }

    @Test
    fun onEventLimitReachSnapshotIsReturned() {
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::", 0, "::topic::", CreateSnapshot(true, Snapshot(0, Binary("::snapshotData::"))))
        )

        val tooBigStringData = "abcdefgh".repeat(200000) //1,600,258 bytes in BSON form

        val eventLimitReachedResponse = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary(tooBigStringData))),
                SaveOptions("::aggregateId::", 1)
        ) as SaveEventsResponse.SnapshotRequired

        assertThat(eventLimitReachedResponse.currentEvents, Is(CoreMatchers.equalTo(listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))))))
        assertThat(eventLimitReachedResponse.currentSnapshot, Is(CoreMatchers.equalTo(Snapshot(1, Binary("::snapshotData::")))))
    }

    @Test
    fun requestingSnapshotSave() {
        val saveEvents = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::", 0, "::topic::", CreateSnapshot(true, Snapshot(0, Binary("::snapshotData::"))))
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents("::aggregateId::")
        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, Is(CoreMatchers.equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        saveEvents.aggregateId,
                                        "Invoice",
                                        Snapshot(0, Binary("::snapshotData::")),
                                        1,
                                        listOf(
                                                EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))
                                        )
                                ))
                        )
                        ))))
            }
            else -> Assert.fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun saveEventsAfterSnapshotChange() {
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::", 0, "::topic::", CreateSnapshot(true, Snapshot(0, Binary("::snapshotData::"))))
        )

        val saveEvents = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))),
                SaveOptions("::aggregateId::", 1)
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents("::aggregateId::")
        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, Is(CoreMatchers.equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        saveEvents.aggregateId,
                                        "Invoice",
                                        Snapshot(0, Binary("::snapshotData::")),
                                        2,
                                        listOf(
                                                EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")),
                                                EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))
                                        )
                                ))
                        )
                        ))))

            }
            else -> Assert.fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun saveManySnapshots() {
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::",
                        Binary("::data::"))),
                SaveOptions("::aggregateId::", 0, "::topic::",
                        CreateSnapshot(true, Snapshot(0, Binary("::snapshotData::"))))
        )

        val response = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::",
                        Binary("::data2::"))),
                SaveOptions("::aggregateId::", 0, "::topic::",
                        CreateSnapshot(true, Snapshot(1, Binary("::snapshotData2::"))))
        ) as SaveEventsResponse.Success

        val success = aggregateBase.getEvents("::aggregateId::")

        when (success) {
            is GetEventsResponse.Success -> {
                assertThat(success, Is(CoreMatchers.equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        response.aggregateId,
                                        "Invoice",
                                        Snapshot(1, Binary("::snapshotData2::")),
                                        1,
                                        listOf(
                                                EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))
                                        )
                                ))
                        )
                        ))))

            }
            else -> Assert.fail("got unknown response when fetching stored events")
        }
    }
}