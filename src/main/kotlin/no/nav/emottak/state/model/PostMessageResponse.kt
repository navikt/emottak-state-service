package no.nav.emottak.state.model

import kotlinx.serialization.Serializable
import kotlin.uuid.Uuid

@Serializable
data class PostMessageResponse(
    val id: Uuid,
    val url: String
)
