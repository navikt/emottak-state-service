package no.nav.emottak.state.service

import no.nav.emottak.state.model.MessageDeliveryState
import no.nav.emottak.state.model.MessageDeliveryState.NEW
import no.nav.emottak.state.model.MessageState
import no.nav.emottak.state.model.MessageStateSnapshot
import no.nav.emottak.state.model.MessageType
import no.nav.emottak.state.repository.MessageRepository
import no.nav.emottak.state.repository.MessageStateHistoryRepository
import no.nav.emottak.state.repository.MessageStateTransactionRepository
import java.net.URL
import kotlin.time.Clock
import kotlin.uuid.Uuid

interface MessageStateService {
    /**
     * Registers a newly accepted message and establishes its initial delivery state.
     *
     * Called when the adapter confirms that a message has been accepted by the external system.
     * The message and its initial [initialState] are recorded together with the external reference
     * and message URL. This marks the message as tracked within the internal domain model.
     *
     * The operation also records an initial state change event so that the message history
     * starts from a consistent baseline.
     *
     * This method is transactional — the message and its history are guaranteed to be updated atomically.
     *
     * @param messageType The domain category of the message (e.g., [MessageType.DIALOG]).
     * @param externalRefId The UUID reference returned from the external API.
     * @param externalMessageUrl The URL that identifies the message in the external system.
     * @param initialState The first known delivery state. Defaults to [MessageDeliveryState.NEW].
     * @return A [MessageStateSnapshot] representing the persisted state and its current history.
     */
    suspend fun createInitialState(
        messageType: MessageType,
        externalRefId: Uuid,
        externalMessageUrl: URL,
        initialState: MessageDeliveryState = NEW
    ): MessageStateSnapshot

    /**
     * Records a new delivery state for an existing message.
     *
     * Called when the external system reports a change in processing or delivery status.
     * The service updates the tracked message’s current state and adds a corresponding
     * history entry describing the transition from [oldState] to [newState].
     *
     * The external message URL provided during creation remains immutable.
     *
     * This method is transactional — both the message state and its history are updated as one logical unit.
     *
     * @param messageType The domain category of the message.
     * @param oldState The message’s previous state
     * @param newState The latest state reported by the external system.
     * @param externalRefId The external system’s unique message identifier.
     * @return A [MessageStateSnapshot] representing the updated message and its full history.
     */
    suspend fun recordStateChange(
        messageType: MessageType,
        oldState: MessageDeliveryState,
        newState: MessageDeliveryState,
        externalRefId: Uuid
    ): MessageStateSnapshot

    /**
     * Retrieves the current snapshot of a tracked message, including its delivery state and full history.
     *
     * Used when inspecting a specific message’s lifecycle — for example, in diagnostics, API queries,
     * or internal monitoring. The returned snapshot includes both the current delivery state
     * and all previously recorded state transitions.
     *
     * @param messageId The unique identifier of the tracked message.
     * @return A [MessageStateSnapshot] containing the message’s current state and full history,
     *         or `null` if no message with the given ID is being tracked.
     */
    suspend fun getMessageSnapshot(messageId: Uuid): MessageStateSnapshot?

    /**
     * Finds messages that are candidates for polling against the external system.
     *
     * Typically returns messages that are still in progress or awaiting confirmation
     * from the external system (e.g., messages in states such as `SENT` or `DELIVERING`).
     * These are used by the poller component to determine which messages should
     * have their external status rechecked.
     *
     * The returned list is limited by the [limit] parameter to prevent excessive batch sizes.
     *
     * @param limit The maximum number of pollable messages to return. Defaults to 100.
     * @return A list of messages that are currently eligible for polling.
     */
    suspend fun findPollableMessages(limit: Int = 100): List<MessageState>
}

class TransactionalMessageStateService(
    private val messageRepository: MessageRepository,
    private val historyRepository: MessageStateHistoryRepository,
    private val transactionRepository: MessageStateTransactionRepository
) : MessageStateService {
    override suspend fun createInitialState(
        messageType: MessageType,
        externalRefId: Uuid,
        externalMessageUrl: URL,
        initialState: MessageDeliveryState
    ): MessageStateSnapshot = transactionRepository
        .createInitialState(
            messageType = messageType,
            initialState = initialState,
            externalRefId = externalRefId,
            externalMessageUrl = externalMessageUrl,
            occurredAt = Clock.System.now()
        )

    override suspend fun recordStateChange(
        messageType: MessageType,
        oldState: MessageDeliveryState,
        newState: MessageDeliveryState,
        externalRefId: Uuid
    ): MessageStateSnapshot = transactionRepository
        .recordStateChange(
            messageType = messageType,
            oldState = oldState,
            newState = newState,
            externalRefId = externalRefId,
            occurredAt = Clock.System.now()
        )

    override suspend fun getMessageSnapshot(messageId: Uuid): MessageStateSnapshot? {
        val state = messageRepository.findOrNull(messageId) ?: return null
        val history = historyRepository.findAll(messageId)

        return MessageStateSnapshot(state, history)
    }

    override suspend fun findPollableMessages(limit: Int): List<MessageState> =
        messageRepository.findForPolling(limit)
}
