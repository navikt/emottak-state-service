package no.nav.emottak.state.repository

import no.nav.emottak.state.model.MessageDeliveryState
import no.nav.emottak.state.model.MessageDeliveryState.NEW
import no.nav.emottak.state.model.MessageState
import no.nav.emottak.state.model.MessageType
import no.nav.emottak.state.repository.Messages.currentState
import no.nav.emottak.state.util.UrlTransformer
import no.nav.emottak.state.util.UuidTransformer
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.datetime.CurrentTimestamp
import org.jetbrains.exposed.v1.datetime.timestamp
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.insertReturning
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.suspendTransaction
import org.jetbrains.exposed.v1.jdbc.updateReturning
import java.net.URL
import kotlin.time.Instant
import kotlin.uuid.Uuid

object Messages : Table("messages") {
    val id = uuid("id").transform(UuidTransformer())

    override val primaryKey = PrimaryKey(id)

    val externalRefId = uuid("external_reference_id")
        .transform(UuidTransformer())
        .uniqueIndex()

    val externalMessageUrl = text("external_message_url").transform(UrlTransformer)
    val messageType = enumerationByName("message_type", 100, MessageType::class)
    val currentState = enumerationByName("current_state", 100, MessageDeliveryState::class)
    val lastStateChange = timestamp("last_state_change")
    val createdAt = timestamp("created_at")
    val updatedAt = timestamp("updated_at")
}

interface MessageRepository {
    suspend fun createState(
        messageType: MessageType,
        state: MessageDeliveryState,
        externalRefId: Uuid,
        externalMessageUrl: URL,
        lastStateChange: Instant
    ): MessageState

    suspend fun updateState(
        messageType: MessageType,
        state: MessageDeliveryState,
        externalRefId: Uuid,
        lastStateChange: Instant
    ): MessageState

    suspend fun findOrNull(id: Uuid): MessageState?

    suspend fun findForPolling(limit: Int = 100): List<MessageState>
}

class ExposedMessageRepository(private val database: Database) : MessageRepository {
    override suspend fun createState(
        messageType: MessageType,
        state: MessageDeliveryState,
        externalRefId: Uuid,
        externalMessageUrl: URL,
        lastStateChange: Instant
    ): MessageState =
        Messages.insertReturning { insert ->
            insert[Messages.messageType] = messageType
            insert[currentState] = state
            insert[Messages.externalRefId] = externalRefId
            insert[Messages.externalMessageUrl] = externalMessageUrl
            insert[Messages.lastStateChange] = lastStateChange
            insert[updatedAt] = CurrentTimestamp
        }
            .single()
            .toMessageState()

    override suspend fun updateState(
        messageType: MessageType,
        state: MessageDeliveryState,
        externalRefId: Uuid,
        lastStateChange: Instant
    ): MessageState =
        Messages.updateReturning(where = { Messages.externalRefId eq externalRefId }) { upsert ->
            upsert[currentState] = state
            upsert[Messages.lastStateChange] = lastStateChange
            upsert[Messages.updatedAt] = CurrentTimestamp
        }
            .single()
            .toMessageState()

    override suspend fun findOrNull(id: Uuid): MessageState? = suspendTransaction(database) {
        Messages
            .selectAll().where { Messages.externalRefId eq id }
            .singleOrNull()
            ?.toMessageState()
    }

    override suspend fun findForPolling(limit: Int): List<MessageState> = suspendTransaction(database) {
        Messages
            .selectAll().where { currentState eq NEW }
            .map { it.toMessageState() }
    }

    private fun ResultRow.toMessageState(): MessageState = MessageState(
        this[Messages.id],
        this[Messages.messageType],
        this[Messages.externalRefId],
        this[Messages.externalMessageUrl],
        this[currentState],
        this[Messages.lastStateChange],
        this[Messages.createdAt],
        this[Messages.updatedAt]
    )
}

class FakeMessageRepository : MessageRepository {
    private val messages = HashMap<Uuid, MessageState>()

    override suspend fun createState(
        messageType: MessageType,
        state: MessageDeliveryState,
        externalRefId: Uuid,
        externalMessageUrl: URL,
        lastStateChange: Instant
    ): MessageState {
        val newMessage = MessageState(
            id = Uuid.random(),
            messageType = messageType,
            currentState = state,
            externalRefId = externalRefId,
            externalMessageUrl = externalMessageUrl,
            lastStateChange = lastStateChange,
            createdAt = lastStateChange,
            updatedAt = lastStateChange
        )
        messages[externalRefId] = newMessage
        return newMessage
    }

    override suspend fun updateState(
        messageType: MessageType,
        state: MessageDeliveryState,
        externalRefId: Uuid,
        lastStateChange: Instant
    ): MessageState {
        val existing = messages[externalRefId]
        val updated = existing!!.copy(
            currentState = state,
            lastStateChange = lastStateChange,
            updatedAt = lastStateChange
        )
        messages[externalRefId] = updated
        return updated
    }

    override suspend fun findOrNull(id: Uuid): MessageState? = messages[id]

    override suspend fun findForPolling(limit: Int): List<MessageState> = messages
        .values
        .filter { it.currentState == NEW }
        .take(limit)
}
