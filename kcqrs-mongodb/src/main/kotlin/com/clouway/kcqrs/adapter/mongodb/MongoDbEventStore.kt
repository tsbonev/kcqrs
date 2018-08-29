package com.clouway.kcqrs.adapter.mongodb

import com.clouway.kcqrs.core.*
import com.clouway.kcqrs.core.messages.MessageFormat
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.ReplaceOptions
import org.bson.Document
import org.bson.RawBsonDocument
import org.bson.codecs.DocumentCodec
import java.io.ByteArrayInputStream

/**
 * @author Tsvetozar Bonev (tsbonev@gmail.com)
 */
class MongoDbEventStore(private val database: String = "db",
                        private val eventsName: String = "Events",
                        private val messageFormat: MessageFormat,
                        private val mongoClient: MongoClient,
                        private val inTestEnvironment: Boolean = false,
                        private val documentSizeLimit: Long = 16793600L) : EventStore {

    /**
     * Collection name used for storing of snapshots.
     */
    private val snapshotsName = eventsName + "Snapshots"

    /**
     * Property name for the aggregate type.
     */
    private val aggregateTypeProperty = "a"

    /**
     * Property name for the list of event data in the document.
     */
    private val eventsProperty = "e"

    /**
     * Property name of the version which is used for concurrency control.
     */
    private val versionProperty = "v"

    /**
     * Collection to store snapshots.
     */
    private val snapshotsCollection: MongoCollection<Document>
        get() = mongoClient.getDatabase(database).getCollection(snapshotsName)

    /**
     * Collection to store aggregates.
     */
    private val eventsCollection: MongoCollection<Document>
        get() = mongoClient.getDatabase(database).getCollection(eventsName)

    override fun saveEvents(aggregateType: String, events: List<EventPayload>, saveOptions: SaveOptions): SaveEventsResponse {
        val aggregateId = saveOptions.aggregateId

        /**
         * If in a test environment, the transactions are
         * stubbed as bwaldvogel:mongo-java-server:1.8.0
         * throws an exception if one is started.
         */
        val session = if(!inTestEnvironment) {
            mongoClient.startSession()
        } else MongoClientSessionStub()

        try{
            session.startTransaction()

            val snapshotDocument = snapshotsCollection.find(eq("_id", aggregateId)).first()
                    ?: Document(mapOf("_id" to aggregateId))

            var aggregateIndex = snapshotDocument.get("aggregateIndex", 0L) as Long


            if (saveOptions.createSnapshot.required && saveOptions.createSnapshot.snapshot != null) {
                aggregateIndex += 1
                val snapshotData = org.bson.types.Binary(saveOptions.createSnapshot.snapshot!!.data.payload)

                snapshotDocument.append("version", saveOptions.createSnapshot.snapshot!!.version)
                snapshotDocument.append("data", snapshotData)
                snapshotDocument.append("aggregateIndex", aggregateIndex)
            }

            val aggregateKey = aggregateKey(aggregateId, aggregateIndex)

            val aggregateDocument =
                eventsCollection.find(eq("_id", aggregateKey)).first()
                        ?: Document(mapOf(
                                "_id" to aggregateKey,
                                eventsProperty to mutableListOf<String>(),
                                versionProperty to 0L,
                                aggregateTypeProperty to aggregateType))

            @Suppress("UNCHECKED_CAST")
            val aggregateEvents = aggregateDocument[eventsProperty] as MutableList<String>
            val currentVersion = aggregateDocument.getLong(versionProperty)

            /**
             *  If the current version is different than what was hydrated during the state change then we know we
             *  have an event collision. This is a very simple approach and more "business knowledge" can be added
             *  here to handle scenarios where the versions may be different but the state change can still occur.
             */
            if (currentVersion != saveOptions.version) {
                return SaveEventsResponse.EventCollision(aggregateId, currentVersion)
            }

            val eventsModel = events.mapIndexed { index, it ->
                EventModel(it.kind,
                        currentVersion + index + 1,
                        it.identityId, it.timestamp,
                        it.data.payload.toString(Charsets.UTF_8)) }

            val eventsAsText = eventsModel.map { (messageFormat.format(it)) }

            aggregateEvents.addAll(eventsAsText)

            aggregateDocument.append(eventsProperty, aggregateEvents)
            aggregateDocument.append(versionProperty, currentVersion + events.size)

            //Current mongodb document size is 16,793,600 bytes
            val docSize = RawBsonDocument(aggregateDocument, DocumentCodec())
                    .byteBuffer.remaining()

            if (docSize >= documentSizeLimit) {
                var snapshot: Snapshot? = null
                //if a build snapshot does not exist it would not have the field data filled in.
                if (snapshotDocument["data"] != null) {
                    val bsonBinaryData = snapshotDocument["data", org.bson.types.Binary::class.java].data
                    snapshot = Snapshot(
                            snapshotDocument.getLong("aggregateIndex") ?: 0L,
                            Binary(bsonBinaryData))
                }
                aggregateEvents.removeAll(eventsAsText)
                return SaveEventsResponse.SnapshotRequired(adaptEvents(aggregateEvents), snapshot)
            }

            eventsCollection.replaceOne(eq("_id", aggregateKey), aggregateDocument, ReplaceOptions().upsert(true))
            snapshotsCollection.replaceOne(eq("_id", aggregateId), snapshotDocument, ReplaceOptions().upsert(true))

            session.commitTransaction()
            return SaveEventsResponse.Success(aggregateId, currentVersion)
        }catch (ex: Exception){
            return SaveEventsResponse.Error("could not save events due: ${ex.message}")
        }finally {
            if(session.hasActiveTransaction()) session.abortTransaction()
            session.close()
        }
    }

    override fun getEvents(aggregateId: String): GetEventsResponse {

        val snapshotDocument = snapshotsCollection.find(eq("_id", aggregateId)).first()
                ?: return GetEventsResponse.SnapshotNotFound

        val aggregateIndex = snapshotDocument.getLong("aggregateIndex") ?: 0L
        val version = snapshotDocument.getLong("version") ?: 0L

        val snapshotData = try {
            snapshotDocument["data", org.bson.types.Binary::class.java]
        } catch (ex: TypeCastException) {
            null
        }

        val snapshot: Snapshot? = if (snapshotData != null) {
            Snapshot(version, Binary(snapshotData.data))
        }else null

        val aggregateKey = aggregateKey(aggregateId, aggregateIndex)

        val aggregate = eventsCollection.find(eq("_id", aggregateKey)).first()
            ?: return GetEventsResponse.AggregateNotFound

        @Suppress("UNCHECKED_CAST")
        val aggregateEvents = aggregate[eventsProperty] as List<String>
        val currentVersion = aggregate.getLong(versionProperty)
        val aggregateType = aggregate.getString(aggregateTypeProperty)

        val events = aggregateEvents.map {
            messageFormat.parse<EventModel>(ByteArrayInputStream(
                    it.toByteArray(Charsets.UTF_8)), EventModel::class.java)
        }.map {
            EventPayload(it.kind, it.timestamp, it.identityId, Binary(it.payload.toByteArray(Charsets.UTF_8)))
        }

        return GetEventsResponse.Success(listOf(Aggregate(aggregateId, aggregateType, snapshot, currentVersion, events)))
    }

    override fun getEvents(aggregateIds: List<String>): GetEventsResponse {

        val snapshotDocuments = mutableMapOf<String, Document>()
        aggregateIds.forEach{
            val snapshot = snapshotsCollection.find(eq("_id", it)).first()
            if(snapshot != null)
            snapshotDocuments[it] = snapshot
        }

        val keyToAggregateId = mutableMapOf<String, String>()

        val aggregateKeys = snapshotDocuments.values.map {
            val key = aggregateKey(it.getString("_id"), it.getLong("aggregateIndex") ?: 0L)
            keyToAggregateId[key] = it.getString("_id")
            key
        }

        val aggregateDocuments = mutableMapOf<String, Document>()
        aggregateKeys.forEach{
            aggregateDocuments[it] = eventsCollection.find(eq("_id", it)).first()!!
        }

        val aggregates = mutableListOf<Aggregate>()

        aggregateDocuments.keys.forEach{
            val aggregateDocument = aggregateDocuments[it]
            var snapshot: Snapshot? = null

            if(snapshotDocuments.containsKey(it)){
                val thisSnapshot = snapshotDocuments[it]!!
                val version = thisSnapshot.getLong("aggregateIndex") ?: 0L
                if (thisSnapshot["data"] != null) {
                    val data = (thisSnapshot["data"] as org.bson.types.Binary).data
                    snapshot = Snapshot(
                            version,
                            Binary(data))
                }
            }

            val aggregateId = keyToAggregateId[it]!!
            val aggregateEvents = aggregateDocument!![eventsProperty] as List<*>
            val currentVersion = aggregateDocument.getLong(versionProperty)
            val aggregateType = aggregateDocument.getString(aggregateTypeProperty)
            val events = adaptEvents(aggregateEvents.filterIsInstance(String::class.java))

            aggregates.add(Aggregate(aggregateId, aggregateType, snapshot, currentVersion, events))
        }

        return GetEventsResponse.Success(aggregates)
    }

    override fun revertLastEvents(aggregateId: String, count: Int): RevertEventsResponse {
        if (count == 0) {
            throw IllegalArgumentException("trying to revert zero events")
        }

        val session = if(!inTestEnvironment) {
            mongoClient.startSession()
        } else MongoClientSessionStub()

        try {
            session.startTransaction()

            val snapshotDocument = snapshotsCollection.find(eq("_id", aggregateId)).first()
                ?: return RevertEventsResponse.AggregateNotFound
            val aggregateIndex = snapshotDocument.getLong("aggregateIndex") ?: 0L

            val aggregateKey = aggregateKey(aggregateId, aggregateIndex)

            val aggregateDocument = eventsCollection.find(eq("_id", aggregateKey)).first()
                ?: return RevertEventsResponse.AggregateNotFound

            @Suppress("UNCHECKED_CAST")
            val aggregateEvents = aggregateDocument[eventsProperty] as MutableList<String>
            val currentVersion = aggregateDocument.getLong(versionProperty)

            if (count > aggregateEvents.size) {
                return RevertEventsResponse.ErrorNotEnoughEventsToRevert
            }

            val lastEventIndex = aggregateEvents.size - count
            val updatedEvents = aggregateEvents.filterIndexed { index, _ -> index < lastEventIndex }

            aggregateDocument[eventsProperty] = updatedEvents
            aggregateDocument[versionProperty] = currentVersion - count

            eventsCollection.replaceOne(eq("_id", aggregateKey), aggregateDocument)
            snapshotsCollection.replaceOne(eq("_id", aggregateId), snapshotDocument)

            session.commitTransaction()
        }catch (ex: Exception){
            return RevertEventsResponse.Error("could not save events due: ${ex.message}")
        }finally {
            if(session.hasActiveTransaction()) session.abortTransaction()
            session.close()
        }

        return RevertEventsResponse.Success
    }

    private fun adaptEvents(aggregateEvents: List<String>): List<EventPayload> {
        return aggregateEvents.map {
            messageFormat.parse<EventModel>(ByteArrayInputStream(it.toByteArray(Charsets.UTF_8)), EventModel::class.java)
        }.map { EventPayload(it.kind, it.timestamp, it.identityId, Binary(it.payload.toByteArray(Charsets.UTF_8))) }
    }

    private fun aggregateKey(aggregateId: String, aggregateIndex: Long) =
            "${aggregateId}_$aggregateIndex"
}