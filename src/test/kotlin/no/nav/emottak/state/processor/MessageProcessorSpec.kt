package no.nav.emottak.state.processor

import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import no.nav.emottak.ediadapter.client.EdiAdapterClient
import no.nav.emottak.ediadapter.model.ErrorMessage
import no.nav.emottak.ediadapter.model.Metadata
import no.nav.emottak.ediadapter.model.PostMessageRequest
import no.nav.emottak.state.DEFAULT_URL
import no.nav.emottak.state.FakeEdiAdapterClient
import no.nav.emottak.state.model.DialogMessage
import no.nav.emottak.state.receiver.MessageReceiver
import no.nav.emottak.state.service.FakeTransactionalMessageStateService
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.uuid.Uuid

class MessageProcessorSpec : StringSpec(
    {

        "Process dialog message - Initialize message state with response from ediAdapterClient" {
            val uuid = Uuid.random()
            val location = "$DEFAULT_URL$uuid"
            val metadata = Metadata(
                id = uuid,
                location = location
            )
            val messageStateService = FakeTransactionalMessageStateService()
            val messageProcessor = MessageProcessor(
                dummyMessageReceiver(),
                messageStateService,
                stubEdiAdapterClient(metadata)
            )

            messageStateService.getMessageSnapshot(uuid).shouldBeNull()

            val dialogMessage = DialogMessage(uuid, "data".toByteArray())
            messageProcessor.processAndSendMessage(dialogMessage)

            val messageSnapshot = messageStateService.getMessageSnapshot(uuid)
            messageSnapshot.shouldNotBeNull()
            messageSnapshot.messageState.externalRefId shouldBeEqual uuid
            messageSnapshot.messageState.externalMessageUrl.toString() shouldBeEqual location
        }
    }
)

private fun stubEdiAdapterClient(metadata: Metadata): EdiAdapterClient = object : FakeEdiAdapterClient() {
    override suspend fun postMessage(postMessagesRequest: PostMessageRequest): Pair<Metadata?, ErrorMessage?> =
        Pair(metadata, null)
}

private fun dummyMessageReceiver(): MessageReceiver = MessageReceiver(
    KafkaReceiver(
        ReceiverSettings(
            bootstrapServers = "",
            keyDeserializer = StringDeserializer(),
            valueDeserializer = ByteArrayDeserializer(),
            groupId = ""
        )
    )
)
