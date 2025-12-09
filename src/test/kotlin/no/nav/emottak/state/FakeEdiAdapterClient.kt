package no.nav.emottak.state

import io.ktor.http.ContentType
import no.nav.emottak.ediadapter.client.EdiAdapterClient
import no.nav.emottak.ediadapter.model.ApprecInfo
import no.nav.emottak.ediadapter.model.ErrorMessage
import no.nav.emottak.ediadapter.model.GetBusinessDocumentResponse
import no.nav.emottak.ediadapter.model.GetMessagesRequest
import no.nav.emottak.ediadapter.model.Message
import no.nav.emottak.ediadapter.model.Metadata
import no.nav.emottak.ediadapter.model.PostAppRecRequest
import no.nav.emottak.ediadapter.model.PostMessageRequest
import no.nav.emottak.ediadapter.model.StatusInfo
import no.nav.emottak.state.processor.BASE64_ENCODING
import kotlin.uuid.Uuid

const val DEFAULT_URL = "https://example.com/messages/"

open class FakeEdiAdapterClient : EdiAdapterClient {
    private val messages = HashMap<Uuid, Message>()
    private val businessDocuments = HashMap<Uuid, String>()

    override suspend fun getApprecInfo(id: Uuid): Pair<List<ApprecInfo>?, ErrorMessage?> {
        return Pair(null, null)
    }

    override suspend fun getMessages(getMessagesRequest: GetMessagesRequest): Pair<List<Message>?, ErrorMessage?> {
        return Pair(null, null)
    }

    override suspend fun postMessage(postMessagesRequest: PostMessageRequest): Pair<Metadata?, ErrorMessage?> {
        val id = Uuid.random()
        val message = Message(
            id = id,
            contentType = postMessagesRequest.contentType,
            receiverHerId = postMessagesRequest.receiverHerIdsSubset?.first()
        )
        messages[id] = message
        businessDocuments[id] = postMessagesRequest.businessDocument
        return Pair(Metadata(id, "$DEFAULT_URL$id"), null)
    }

    override suspend fun getMessage(id: Uuid): Pair<Message?, ErrorMessage?> {
        return Pair(messages[id], null)
    }

    override suspend fun getBusinessDocument(id: Uuid): Pair<GetBusinessDocumentResponse?, ErrorMessage?> {
        val documentResponse = businessDocuments[id]?.let {
            GetBusinessDocumentResponse(
                businessDocument = it,
                contentType = ContentType.Application.Xml.toString(),
                contentTransferEncoding = BASE64_ENCODING
            )
        }

        return Pair(documentResponse, null)
    }

    override suspend fun getMessageStatus(id: Uuid): Pair<List<StatusInfo>?, ErrorMessage?> {
        return Pair(null, null)
    }

    override suspend fun postApprec(
        id: Uuid,
        apprecSenderHerId: Int,
        postAppRecRequest: PostAppRecRequest
    ): Pair<Metadata?, ErrorMessage?> {
        return Pair(null, null)
    }

    override suspend fun markMessageAsRead(id: Uuid, herId: Int): Pair<Boolean?, ErrorMessage?> {
        return Pair(null, null)
    }

    override fun close() {
    }
}
