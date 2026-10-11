from bots.serializers import CalendarSerializer, ChatMessageSerializer, ParticipantEventSerializer, ZoomOAuthConnectionSerializer


def chat_message_webhook_payload(chat_message):
    return ChatMessageSerializer(chat_message).data


def utterance_webhook_payload(utterance):
    return {
        "speaker_name": utterance.participant.full_name,
        "speaker_uuid": utterance.participant.uuid,
        "speaker_user_uuid": utterance.participant.user_uuid,
        "speaker_is_host": utterance.participant.is_host,
        "timestamp_ms": utterance.timestamp_ms,
        "duration_ms": utterance.duration_ms,
        "transcription": {"transcript": utterance.transcription.get("transcript")} if utterance.transcription else None,
    }


def partial_transcript_webhook_payload(participant_data, transcript, timestamp_ms):
    """The text of an utterance still being spoken (transcript.partial); the final one comes as transcript.update."""
    return {
        "speaker_name": participant_data.get("participant_full_name"),
        "speaker_uuid": participant_data.get("participant_uuid"),
        "speaker_user_uuid": participant_data.get("participant_user_uuid"),
        "speaker_is_host": participant_data.get("participant_is_host", False),
        "timestamp_ms": timestamp_ms,
        "transcription": {"transcript": transcript},
    }


def participant_event_webhook_payload(participant_event):
    return ParticipantEventSerializer(participant_event).data


def calendar_webhook_payload(calendar):
    serialized_calendar = CalendarSerializer(calendar).data
    return {
        "state": serialized_calendar["state"],
        "connection_failure_data": serialized_calendar["connection_failure_data"],
        "last_successful_sync_at": serialized_calendar["last_successful_sync_at"],
        "last_attempted_sync_at": serialized_calendar["last_attempted_sync_at"],
    }


def zoom_oauth_connection_webhook_payload(zoom_oauth_connection):
    serialized_zoom_oauth_connection = ZoomOAuthConnectionSerializer(zoom_oauth_connection).data
    return {
        "state": serialized_zoom_oauth_connection["state"],
        "connection_failure_data": serialized_zoom_oauth_connection["connection_failure_data"],
        "last_successful_sync_at": serialized_zoom_oauth_connection["last_successful_sync_at"],
        "last_attempted_sync_at": serialized_zoom_oauth_connection["last_attempted_sync_at"],
    }
