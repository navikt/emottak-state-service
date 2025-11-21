package no.nav.emottak.state.service

import io.github.nomisRev.kafka.receiver.AutoOffsetReset
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import no.nav.emottak.utils.config.Kafka
import no.nav.emottak.utils.config.toProperties
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.time.Duration.Companion.seconds

// TODO: can we use a global log instance instead? How does Kotlin/Ktor treat this?
private val log = KotlinLogging.logger {}

suspend fun startPayloadReceiver(
    topic: String,
    kafka: Kafka
) {
    log.info { "Starting payload message receiver on topic $topic" }
    val receiverSettings: ReceiverSettings<String, ByteArray> =
        ReceiverSettings(
            bootstrapServers = kafka.bootstrapServers,
            keyDeserializer = StringDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            groupId = kafka.groupId,
            autoOffsetReset = AutoOffsetReset.Latest,
            pollTimeout = 10.seconds,
            properties = kafka.toProperties()
        )

    KafkaReceiver(receiverSettings)
        .receive(topic)
        .map { record ->
            val valueString = try {
                String(record.value())
            } catch (e: Exception) {
                "[unreadable bytes]"
            }
            log.info { "Received message with key ${record.key()} and value $valueString" }
        }.collect()
}
