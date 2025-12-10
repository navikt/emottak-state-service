package no.nav.emottak.state.poller

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import no.nav.emottak.ediadapter.client.EdiAdapterClient
import no.nav.emottak.ediadapter.model.DeliveryState
import no.nav.emottak.state.FakeEdiAdapterClient
import no.nav.emottak.state.evaluator.StateEvaluator
import no.nav.emottak.state.evaluator.StateTransitionValidator
import no.nav.emottak.state.model.AppRecStatus.OK
import no.nav.emottak.state.model.AppRecStatus.REJECTED
import no.nav.emottak.state.model.CreateState
import no.nav.emottak.state.model.ExternalDeliveryState.ACKNOWLEDGED
import no.nav.emottak.state.model.MessageType.DIALOG
import no.nav.emottak.state.publisher.FakeDialogMessagePublisher
import no.nav.emottak.state.publisher.MessagePublisher
import no.nav.emottak.state.service.FakeTransactionalMessageStateService
import no.nav.emottak.state.service.MessageStateService
import no.nav.emottak.state.service.PollerService
import no.nav.emottak.state.service.StateEvaluatorService
import java.net.URI
import kotlin.uuid.Uuid
import no.nav.emottak.ediadapter.model.AppRecStatus as ExternalAppRecStatus

class PollerServiceSpec : StringSpec(
    {
        "No state change → no publish" {
            val (ediAdapterClient, messageStateService, dialogMessagePublisher, pollerService) = fixture()

            val externalRefId = Uuid.random()
            val externalUrl = URI("http://example.com/1").toURL()

            val messageSnapshot = messageStateService.createInitialState(
                CreateState(
                    DIALOG,
                    externalRefId,
                    externalUrl
                )
            )
            ediAdapterClient.givenStatus(externalRefId, DeliveryState.ACKNOWLEDGED, null)

            pollerService.pollAndProcessMessage(messageSnapshot.messageState)

            val currentMessageSnapshot = messageStateService.getMessageSnapshot(externalRefId)!!
            currentMessageSnapshot.messageState.externalDeliveryState shouldBe ACKNOWLEDGED
            currentMessageSnapshot.messageState.appRecStatus shouldBe null

            dialogMessagePublisher.published shouldBe emptyList()
        }

        "PENDING → COMPLETED publishes update" {
            val (ediAdapterClient, messageStateService, dialogMessagePublisher, pollerService) = fixture()

            val externalRefId = Uuid.random()
            val externalUrl = URI("http://example.com/1").toURL()

            val messageSnapshot = messageStateService.createInitialState(
                CreateState(
                    DIALOG,
                    externalRefId,
                    externalUrl
                )
            )

            ediAdapterClient.givenStatus(externalRefId, DeliveryState.ACKNOWLEDGED, null)

            val messageState = messageSnapshot.messageState
            pollerService.pollAndProcessMessage(messageState)

            ediAdapterClient.givenStatus(externalRefId, DeliveryState.ACKNOWLEDGED, ExternalAppRecStatus.OK)

            pollerService.pollAndProcessMessage(messageState)

            dialogMessagePublisher.published.size shouldBe 1
            dialogMessagePublisher.published.first().referenceId shouldBe externalRefId
            dialogMessagePublisher.published.first().appRecStatus shouldBe OK
        }

        "External REJECTED → publish rejection" {
            val (ediAdapterClient, messageStateService, dialogMessagePublisher, pollerService) = fixture()

            val externalRefId = Uuid.random()
            val externalUrl = URI("http://example.com/1").toURL()

            val messageSnapshot = messageStateService.createInitialState(
                CreateState(
                    DIALOG,
                    externalRefId,
                    externalUrl
                )
            )
            ediAdapterClient.givenStatus(externalRefId, DeliveryState.REJECTED, null)

            pollerService.pollAndProcessMessage(messageSnapshot.messageState)

            dialogMessagePublisher.published.size shouldBe 1
            dialogMessagePublisher.published.first().referenceId shouldBe externalRefId
            dialogMessagePublisher.published.first().appRecStatus shouldBe REJECTED
        }

        "Unresolvable external state → INVALID but not published" {
            val (ediAdapterClient, messageStateService, dialogMessagePublisher, pollerService) = fixture()

            val externalRefId = Uuid.random()
            val externalUrl = URI("http://example.com/1").toURL()

            val messageSnapshot = messageStateService.createInitialState(
                CreateState(
                    DIALOG,
                    externalRefId,
                    externalUrl
                )
            )

            ediAdapterClient.givenStatus(externalRefId, DeliveryState.UNCONFIRMED, ExternalAppRecStatus.REJECTED)

            val messageState = messageSnapshot.messageState

            pollerService.pollAndProcessMessage(messageState)

            val currentMessageSnapshot = messageStateService.getMessageSnapshot(externalRefId)!!
            currentMessageSnapshot.messageState.externalDeliveryState shouldBe null
            currentMessageSnapshot.messageState.appRecStatus shouldBe null

            dialogMessagePublisher.published shouldBe emptyList()
        }
    }
)

private data class Fixture(
    val ediAdapterClient: FakeEdiAdapterClient,
    val messageStateService: FakeTransactionalMessageStateService,
    val dialogMessagePublisher: FakeDialogMessagePublisher,
    val pollerService: PollerService
)

private fun fixture(): Fixture {
    val ediAdapterClient = FakeEdiAdapterClient()
    val messageStateService = FakeTransactionalMessageStateService()
    val dialogMessagePublisher = FakeDialogMessagePublisher()

    return Fixture(
        ediAdapterClient,
        messageStateService,
        dialogMessagePublisher,
        pollerService(
            ediAdapterClient,
            messageStateService,
            dialogMessagePublisher
        )
    )
}

private fun pollerService(
    ediAdapterClient: EdiAdapterClient,
    messageStateService: MessageStateService,
    messagePublisher: MessagePublisher
): PollerService = PollerService(
    ediAdapterClient,
    messageStateService,
    stateEvaluatorService(),
    messagePublisher
)

private fun stateEvaluatorService(): StateEvaluatorService = StateEvaluatorService(
    StateEvaluator(),
    StateTransitionValidator()
)
