package no.nav.emottak.state.model

import kotlinx.serialization.Serializable
import kotlin.uuid.Uuid

@Serializable
data class DialogMessage(
    val id: Uuid,
    val payload: ByteArray
)
