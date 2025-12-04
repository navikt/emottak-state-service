package no.nav.emottak.state.plugin

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.micrometer.prometheus.PrometheusMeterRegistry
import no.nav.emottak.ediadapter.client.EdiAdapterClient
import no.nav.emottak.ediadapter.client.scopedAuthHttpClient
import no.nav.emottak.ediadapter.model.AppRecError
import no.nav.emottak.ediadapter.model.AppRecStatus
import no.nav.emottak.ediadapter.model.EbXmlInfo
import no.nav.emottak.ediadapter.model.GetMessagesRequest
import no.nav.emottak.ediadapter.model.OrderBy
import no.nav.emottak.ediadapter.model.PostAppRecRequest
import no.nav.emottak.ediadapter.model.PostMessageRequest
import org.slf4j.LoggerFactory
import kotlin.uuid.Uuid

fun Application.configureRoutes(
    registry: PrometheusMeterRegistry
) {
    routing { internalRoutes(registry) }
}

fun Route.internalRoutes(registry: PrometheusMeterRegistry) {
    get("/prometheus") {
        call.respond(registry.scrape())
    }
    route("/internal") {
        get("/health/liveness") {
            call.respondText("I'm alive! :)")
        }
        get("/health/readiness") {
            call.respondText("I'm ready! :)")
        }
    }

    get("/edi2-test") {
        val log = LoggerFactory.getLogger("no.nav.emottak.state.App")

        val scope = "api://dev-gcp.team-emottak.edi-adapter/.default"
        val ediAdapterUrl = "https://edi-transport.intern.dev.nav.no"

        val scopedClient = scopedAuthHttpClient(scope)
        val ediAdapterClient = EdiAdapterClient(ediAdapterUrl, scopedClient)

        // 1
        try {
            val response = ediAdapterClient.getApprecInfo(Uuid.random())
            log.info("EDI2 test: Response from edi-adapter: $response")
            log.info("EDI2 test: test succeeded getApprecInfo()")
        } catch (e: Exception) {
            log.error("EDI2 test: Exception occurred while calling edi-adapter", e)
            log.info("EDI2 test: test failed getApprecInfo()")
        }

        // 2
        try {
            val getMessagesRequest = GetMessagesRequest(
                receiverHerIds = listOf(123456),
                senderHerId = 654321,
                businessDocumentId = Uuid.random().toString(),
                includeMetadata = true,
                messagesToFetch = 5,
                orderBy = OrderBy.DESC
            )

            val response = ediAdapterClient.getMessages(getMessagesRequest)
            log.info("EDI2 test: Response from edi-adapter: $response")
            log.info("EDI2 test: test succeeded getMessages()")
        } catch (e: Exception) {
            log.error("EDI2 test: Exception occurred while calling edi-adapter", e)
            log.info("EDI2 test: test failed getMessages()")
        }

        // 3
        try {
            val postMessagesRequest = PostMessageRequest(
                businessDocument = "<test>Dette er en test</test>",
                contentType = "application/xml",
                contentTransferEncoding = "base64",
                ebXmlOverrides = EbXmlInfo(
                    cpaId = "test-cpa-id",
                    conversationId = "test-conversation-id",
                    service = "test-service",
                    serviceType = "test-service-type",
                    action = "test-action",
                    role = "test-sender-role",
                    useSenderLevel1HerId = true,
                    receiverRole = "test-receiver-role",
                    applicationName = "test-application-name",
                    applicationVersion = "1.0",
                    middlewareName = "test-middleware-name",
                    middlewareVersion = "1.0",
                    compressPayload = false
                ),
                receiverHerIdsSubset = listOf(123456)
            )

            val response = ediAdapterClient.postMessage(postMessagesRequest)
            log.info("EDI2 test: Response from edi-adapter: $response")
            log.info("EDI2 test: test succeeded postMessages()")
        } catch (e: Exception) {
            log.error("EDI2 test: Exception occurred while calling edi-adapter", e)
            log.info("EDI2 test: test failed postMessages()")
        }

        // 4
        try {
            val response = ediAdapterClient.getMessage(Uuid.random())
            log.info("EDI2 test: Response from edi-adapter: $response")
            log.info("EDI2 test: test succeeded getMessage()")
        } catch (e: Exception) {
            log.error("EDI2 test: Exception occurred while calling edi-adapter", e)
            log.info("EDI2 test: test failed getMessage()")
        }

        // 5
        try {
            val response = ediAdapterClient.getBusinessDocument(Uuid.random())
            log.info("EDI2 test: Response from edi-adapter: $response")
            log.info("EDI2 test: test succeeded getBusinessDocument()")
        } catch (e: Exception) {
            log.error("EDI2 test: Exception occurred while calling edi-adapter", e)
            log.info("EDI2 test: test failed getBusinessDocument()")
        }

        // 6
        try {
            val response = ediAdapterClient.getMessageStatus(Uuid.random())
            log.info("EDI2 test: Response from edi-adapter: $response")
            log.info("EDI2 test: test succeeded getMessageStatus()")
        } catch (e: Exception) {
            log.error("EDI2 test: Exception occurred while calling edi-adapter", e)
            log.info("EDI2 test: test failed getMessageStatus()")
        }

        // 7
        try {
            val postAppRecRequest = PostAppRecRequest(
                appRecStatus = AppRecStatus.REJECTED,
                appRecErrorList = listOf(
                    AppRecError(
                        errorCode = "123",
                        details = "Some details",
                        description = "Some description",
                        oid = "abc123"
                    )
                ),
                ebXmlOverrides = EbXmlInfo(
                    cpaId = "test-cpa-id",
                    conversationId = "test-conversation-id",
                    service = "test-service",
                    serviceType = "test-service-type",
                    action = "test-action",
                    role = "test-sender-role",
                    useSenderLevel1HerId = true,
                    receiverRole = "test-receiver-role",
                    applicationName = "test-application-name",
                    applicationVersion = "1.0",
                    middlewareName = "test-middleware-name",
                    middlewareVersion = "1.0",
                    compressPayload = false
                )
            )

            val response = ediAdapterClient.postApprec(Uuid.random(), 123456, postAppRecRequest)
            log.info("EDI2 test: Response from edi-adapter: $response")
            log.info("EDI2 test: test succeeded postApprec()")
        } catch (e: Exception) {
            log.error("EDI2 test: Exception occurred while calling edi-adapter", e)
            log.info("EDI2 test: test failed postApprec()")
        }

        // 8
        try {
            val response = ediAdapterClient.markMessageAsRead(Uuid.random(), 123456)
            log.info("EDI2 test: Response from edi-adapter: $response")
            log.info("EDI2 test: test succeeded markMessageAsRead()")
        } catch (e: Exception) {
            log.error("EDI2 test: Exception occurred while calling edi-adapter", e)
            log.info("EDI2 test: test failed markMessageAsRead()")
        }

        call.respond(
            HttpStatusCode.OK,
            "EDI2 test: Pong from edi2-test"
        )
    }
}
