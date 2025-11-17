package no.nav.emottak.state.service

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import io.github.oshai.kotlinlogging.KotlinLogging
import no.nav.emottak.state.model.MessageDeliveryState
import no.nav.emottak.state.model.MessageType
import no.nav.emottak.state.repository.MessageStateHistoryRepository
import no.nav.emottak.state.util.toUrlOrNull
import no.nav.emottak.state.util.toUuidOrNull
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.net.URI
import java.net.URL
import java.time.Duration
import java.util.Properties
import kotlin.time.Clock
import kotlin.uuid.Uuid

class Fag2EdiService(
    private val messageStateService: MessageStateService,
    private val messageStateHistoryRepository: MessageStateHistoryRepository,
    private val ediClient: Any, // client from emottak-utils
    private val kafkaProps: Properties,
    private val topic: String,
    private val scope: CoroutineScope
) {

    private val log = KotlinLogging.logger {}

    @Suppress("unused")
    fun start() {
        log.info { "Starting Fag2EdiService kafka consumer for topic: $topic" }
        ensureKafkaDefaults()

        scope.launch {
            KafkaConsumer<String, String>(kafkaProps).use { consumer ->
                consumer.subscribe(listOf(topic))

                try {
                    while (isActive) {
                        val records = consumer.poll(Duration.ofSeconds(1))
                        if (!records.isEmpty) {
                            for (record in records) {
                                try {
                                    val payload = record.value()
                                    launch { handleIncoming(payload) }
                                } catch (ex: Exception) {
                                    log.error(ex) { "Failed to process record" }
                                }
                            }
                            consumer.commitSync()
                        }
                    }
                } catch (ex: Exception) {
                    log.error(ex) { "Kafka consumer loop stopped" }
                }
            }
        }
    }

    private fun ensureKafkaDefaults() {
        kafkaProps.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        kafkaProps.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        kafkaProps.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    }

    private suspend fun handleIncoming(payload: String) {
        val externalRefId: Uuid = payload.toUuidOrNull() ?: Uuid.random()

        val externalMessageUrl: URL = payload.toUrlOrNull() ?: run {
            log.debug { "Payload is not a URL; using placeholder externalMessageUrl" }
            URI("http://unknown").toURL()
        }

        val snapshot = messageStateService.createInitialState(
            MessageType.DIALOG,
            externalRefId,
            externalMessageUrl,
            MessageDeliveryState.NEW
        )

        val stateId: Uuid = snapshot.messageState.id

        try {
            persistState(stateId)
        } catch (ex: Exception) {
            log.error(ex) { "Persisting state failed for $stateId" }
        }

        try {
            pushToEdi(stateId)
        } catch (ex: Exception) {
            log.error(ex) { "Pushing to EDI failed for $stateId" }
        }
    }

    private suspend fun persistState(stateId: Uuid) {
        messageStateHistoryRepository.append(
            stateId,
            null,
            MessageDeliveryState.NEW,
            Clock.System.now()
        )
        log.debug { "Persisted messageState history for $stateId" }
    }

    private suspend fun pushToEdi(stateId: Uuid) {
        // TODO: Replace with actual ediClient method call when available
    }
}
