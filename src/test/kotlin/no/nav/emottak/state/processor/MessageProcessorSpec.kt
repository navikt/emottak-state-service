package no.nav.emottak.state.processor

import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import no.nav.emottak.ediadapter.model.ErrorMessage
import no.nav.emottak.ediadapter.model.Metadata
import no.nav.emottak.state.FakeEdiAdapterClient
import no.nav.emottak.state.model.DialogMessage
import no.nav.emottak.state.receiver.MessageReceiver
import no.nav.emottak.state.service.FakeTransactionalMessageStateService
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.uuid.Uuid

class MessageProcessorSpec : StringSpec(
    {

        "create state if ediAdapterClient returns metadata and no error message" {
            val messageStateService = FakeTransactionalMessageStateService()
            val ediAdapterClient = FakeEdiAdapterClient()
            val messageProcessor = MessageProcessor(
                dummyMessageReceiver(),
                messageStateService,
                ediAdapterClient
            )

            val uuid = Uuid.random()
            val location = "https://example.com/messages/$uuid"
            val metadata = Metadata(
                id = uuid,
                location = location
            )
            ediAdapterClient.givenPostMessage(Pair(metadata, null))

            messageStateService.getMessageSnapshot(uuid).shouldBeNull()

            val dialogMessage = DialogMessage(uuid, "data".toByteArray())
            messageProcessor.processAndSendMessage(dialogMessage)

            val messageSnapshot = messageStateService.getMessageSnapshot(uuid)
            messageSnapshot.shouldNotBeNull()
            messageSnapshot.messageState.externalRefId shouldBeEqual uuid
            messageSnapshot.messageState.externalMessageUrl.toString() shouldBeEqual location
        }

        "no state created if ediAdapterClient returns error message and no metadata" {
            val messageStateService = FakeTransactionalMessageStateService()
            val ediAdapterClient = FakeEdiAdapterClient()
            val messageProcessor = MessageProcessor(
                dummyMessageReceiver(),
                messageStateService,
                ediAdapterClient
            )

            val uuid = Uuid.random()

            val errorMessage500 = ErrorMessage(
                error = "Internal Server Error",
                errorCode = 1000,
                validationErrors = listOf("Example error"),
                stackTrace = "[StackTrace]",
                requestId = Uuid.random().toString()
            )

            ediAdapterClient.givenPostMessage(Pair(null, errorMessage500))

            messageStateService.getMessageSnapshot(uuid).shouldBeNull()

            val dialogMessage = DialogMessage(uuid, "data".toByteArray())
            messageProcessor.processAndSendMessage(dialogMessage)

            messageStateService.getMessageSnapshot(uuid).shouldBeNull()
        }

        "no state created if ediAdapterClient returns no metadata nor error message" {
            val messageStateService = FakeTransactionalMessageStateService()
            val ediAdapterClient = FakeEdiAdapterClient()
            val messageProcessor = MessageProcessor(
                dummyMessageReceiver(),
                messageStateService,
                ediAdapterClient
            )

            val uuid = Uuid.random()

            ediAdapterClient.givenPostMessage(Pair(null, null))

            messageStateService.getMessageSnapshot(uuid).shouldBeNull()

            val dialogMessage = DialogMessage(uuid, "data".toByteArray())
            messageProcessor.processAndSendMessage(dialogMessage)

            messageStateService.getMessageSnapshot(uuid).shouldBeNull()
        }

        "no state created if ediAdapterClient returns metadata and error message" {
            val messageStateService = FakeTransactionalMessageStateService()
            val ediAdapterClient = FakeEdiAdapterClient()
            val messageProcessor = MessageProcessor(
                dummyMessageReceiver(),
                messageStateService,
                ediAdapterClient
            )

            val uuid = Uuid.random()
            val location = "https://example.com/messages/$uuid"
            val metadata = Metadata(
                id = uuid,
                location = location
            )
            val errorMessage500 = ErrorMessage(
                error = "Internal Server Error",
                errorCode = 1000,
                validationErrors = listOf("Example error"),
                stackTrace = "[StackTrace]",
                requestId = Uuid.random().toString()
            )

            ediAdapterClient.givenPostMessage(Pair(metadata, errorMessage500))

            messageStateService.getMessageSnapshot(uuid).shouldBeNull()

            val dialogMessage = DialogMessage(uuid, "data".toByteArray())
            messageProcessor.processAndSendMessage(dialogMessage)

            messageStateService.getMessageSnapshot(uuid).shouldBeNull()
        }
    }
)

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
