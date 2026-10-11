from types import SimpleNamespace
from unittest import TestCase, mock

from bots.transcription_providers.deepgram import deepgram_streaming_transcriber as module
from bots.transcription_providers.deepgram.deepgram_streaming_transcriber import DeepgramStreamingTranscriber


def result(text, start, duration, is_final=True, speech_final=False, from_finalize=False):
    return SimpleNamespace(channel=SimpleNamespace(alternatives=[SimpleNamespace(transcript=text)]), start=start, duration=duration, is_final=is_final, speech_final=speech_final, from_finalize=from_finalize)


class DeepgramStreamingTranscriberTest(TestCase):
    def make(self, **kwargs):
        with mock.patch.object(module, "DeepgramClient") as client:
            connection = client.return_value.listen.websocket.v.return_value
            transcriber = DeepgramStreamingTranscriber(deepgram_api_key="key", interim_results=True, language="en", model="nova-3", sample_rate=16000, metadata=None, callback=None, **kwargs)
        return transcriber, connection

    def saved(self):
        module._save_queue.join()

    def test_passes_keyterms_and_endpointing_to_deepgram(self):
        _, connection = self.make(keyterms=["Gus", "Willow"], endpointing=300)
        options = connection.start.call_args.args[0]
        self.assertEqual(options.keyterm, ["Gus", "Willow"])
        self.assertEqual(options.endpointing, 300)

    def test_saves_one_utterance_per_end_of_speech(self):
        saved = []
        transcriber, _ = self.make(save_utterance_callback=lambda text, meta: saved.append((text, meta)))
        with mock.patch.object(module.time, "time", return_value=1000.0):
            transcriber.send(b"\0\0")
        transcriber.handle_result(result("Gus,", 1.0, 0.4, is_final=False))  # interim: ignored
        transcriber.handle_result(result("Gus,", 1.0, 0.4))
        transcriber.handle_result(result("is Willow's budget approved?", 1.4, 1.6, speech_final=True))
        transcriber.handle_result(result("Thanks.", 5.0, 0.5, speech_final=True))
        self.saved()
        self.assertEqual(saved, [
            ("Gus, is Willow's budget approved?", {"timestamp_ms": 1001000, "duration_ms": 2000}),
            ("Thanks.", {"timestamp_ms": 1005000, "duration_ms": 500}),
        ])

    def test_finish_saves_speech_cut_off_by_the_end_of_the_stream(self):
        saved = []
        transcriber, connection = self.make(save_utterance_callback=lambda text, meta: saved.append(text))
        transcriber.send(b"\0\0")
        transcriber.handle_result(result("Let's go with Cedar", 0.5, 1.2))
        transcriber.finish()
        self.saved()
        connection.finish.assert_called_once()
        self.assertEqual(saved, ["Let's go with Cedar"])

    def test_a_quiet_speaker_is_finalized_once_and_the_utterance_saved(self):
        saved = []
        transcriber, connection = self.make(save_utterance_callback=lambda text, meta: saved.append(text))
        transcriber.send(b"\0\0")
        transcriber.finalize_if_quiet()
        connection.finalize.assert_not_called()  # nothing heard yet
        transcriber.handle_result(result("Do we have a date for the founder", 0.5, 2.0, is_final=False))
        transcriber.finalize_if_quiet()
        transcriber.finalize_if_quiet()
        connection.finalize.assert_called_once()
        transcriber.handle_result(result("Do we have a date for the founder interview?", 0.5, 2.4, from_finalize=True))
        self.saved()
        self.assertEqual(saved, ["Do we have a date for the founder interview?"])

    def test_partial_text_while_speaking_is_throttled_and_stops_at_the_end(self):
        partials, saved = [], []
        transcriber, _ = self.make(save_utterance_callback=lambda text, meta: saved.append(text), partial_callback=lambda text, meta: partials.append((text, meta)))
        with mock.patch.object(module.time, "time", return_value=1000.0):
            transcriber.send(b"\0\0")
        clock = [1000.0]
        with mock.patch.object(module.time, "time", side_effect=lambda: clock[0]):
            transcriber.handle_result(result("Gus,", 1.0, 0.4, is_final=False))
            clock[0] += 0.1
            transcriber.handle_result(result("Gus, is", 1.0, 0.6, is_final=False))  # within 250 ms: skipped
            clock[0] += 0.3
            transcriber.handle_result(result("Gus,", 1.0, 0.4))  # final piece: the text so far
            clock[0] += 0.3
            transcriber.handle_result(result("is Willow's budget", 1.4, 1.0, is_final=False))
            clock[0] += 0.3
            transcriber.handle_result(result("is Willow's budget approved?", 1.4, 1.6, speech_final=True))
        self.saved()
        self.assertEqual([p[0] for p in partials], ["Gus,", "Gus, is Willow's budget"])
        self.assertEqual(partials[0][1], {"timestamp_ms": 1001000})
        self.assertEqual(saved, ["Gus, is Willow's budget approved?"])

    def test_with_a_callback_url_deepgram_delivers_and_nothing_is_saved_here(self):
        transcriber, _ = self.make()
        transcriber.handle_result(result("Hello", 0.0, 0.5, speech_final=True))
        self.assertEqual(transcriber.pending_text, [])


class DeepgramStreamingSettingsTest(TestCase):
    def test_streaming_without_a_callback(self):
        from bots.models import TranscriptionSettings

        self.assertFalse(TranscriptionSettings({"deepgram": {"language": "en"}}).deepgram_use_streaming())
        self.assertTrue(TranscriptionSettings({"deepgram": {"callback": "https://example.com/dg"}}).deepgram_use_streaming())
        settings = TranscriptionSettings({"deepgram": {"streaming": True, "endpointing": 300, "keyterms": ["Gus"]}})
        self.assertTrue(settings.deepgram_use_streaming())
        self.assertEqual(settings.deepgram_endpointing(), 300)
        self.assertIsNone(TranscriptionSettings({"deepgram": {}}).deepgram_endpointing())

    def test_schema_accepts_streaming_and_endpointing(self):
        import jsonschema

        from bots.serializers import TRANSCRIPTION_SETTINGS_SCHEMA

        jsonschema.validate({"deepgram": {"streaming": True, "endpointing": 300, "partial_webhooks": True, "keyterms": ["Gus"], "language": "en"}}, TRANSCRIPTION_SETTINGS_SCHEMA)
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate({"deepgram": {"endpointing": 5}}, TRANSCRIPTION_SETTINGS_SCHEMA)


class PartialTranscriptWebhookTest(TestCase):
    def test_trigger_code_and_payload(self):
        from bots.models import WebhookTriggerTypes
        from bots.webhook_payloads import partial_transcript_webhook_payload

        self.assertEqual(WebhookTriggerTypes.api_code_to_trigger_type("transcript.partial"), WebhookTriggerTypes.TRANSCRIPT_PARTIAL)
        payload = partial_transcript_webhook_payload({"participant_full_name": "Ana", "participant_uuid": "u1", "participant_user_uuid": None, "participant_is_host": True}, "Gus, is", 123)
        self.assertEqual(payload, {"speaker_name": "Ana", "speaker_uuid": "u1", "speaker_user_uuid": None, "speaker_is_host": True, "timestamp_ms": 123, "transcription": {"transcript": "Gus, is"}})
