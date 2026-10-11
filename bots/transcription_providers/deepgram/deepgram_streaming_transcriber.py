import logging
import queue
import threading
import time

from deepgram import (
    DeepgramClient,
    DeepgramClientOptions,
    LiveOptions,
    LiveTranscriptionEvents,
)

logger = logging.getLogger(__name__)

# Utterances from every speaker are saved by one thread, in order, as Kyutai's are:
# Deepgram's results arrive on its websocket threads.
_save_queue = queue.Queue()
_save_thread = None
_save_thread_lock = threading.Lock()


def _enqueue_save(func):
    global _save_thread
    with _save_thread_lock:
        if _save_thread is None:

            def consume():
                while True:
                    job = _save_queue.get()
                    try:
                        job()
                    except Exception as e:
                        logger.error(f"Error saving Deepgram streaming utterance: {e}", exc_info=True)
                    finally:
                        _save_queue.task_done()

            _save_thread = threading.Thread(target=consume, daemon=True, name="deepgram-utterance-saver")
            _save_thread.start()
    _save_queue.put(func)


class DeepgramStreamingTranscriber:
    """
    Streams one participant's audio to Deepgram.

    With a Deepgram callback URL, Deepgram sends the results straight to that URL.
    Otherwise, pass save_utterance_callback: final results are joined into an
    utterance, which is saved (and sent as a transcript.update webhook) as soon as
    Deepgram marks the end of speech (speech_final).
    """

    def __init__(self, *, deepgram_api_key, interim_results, language, model, sample_rate, metadata, callback, redaction_settings=None, replace_settings=None, mip_opt_out=None, keyterms=None, endpointing=None, save_utterance_callback=None):
        # Configure the DeepgramClientOptions to enable KeepAlive for maintaining the WebSocket connection (only if necessary to your scenario)
        config = DeepgramClientOptions(options={"keepalive": "true"})

        self.last_send_time = time.time()
        self.save_utterance_callback = save_utterance_callback
        # Wall-clock time of the first audio sent; Deepgram's result times are offsets from it.
        self.stream_start_time = None
        self.pending_text = []
        self.pending_start = None
        self.pending_end = None
        # Text heard since the last Finalize request (see finalize_if_quiet).
        self.heard_since_finalize = False

        # Create a websocket connection using the DEEPGRAM_API_KEY from environment variables
        self.deepgram = DeepgramClient(deepgram_api_key, config)

        # Use the listen.live class to create the websocket connection
        self.dg_connection = self.deepgram.listen.websocket.v("1")

        transcriber = self

        def on_message(self, result, **kwargs):
            sentence = result.channel.alternatives[0].transcript
            if len(sentence) > 0:
                logger.info(f"Transcription: {sentence}")
            transcriber.handle_result(result)

        self.dg_connection.on(LiveTranscriptionEvents.Transcript, on_message)

        def on_error(self, error, **kwargs):
            logger.error(f"Error in Deepgram streaming transcription: {error}")

        self.dg_connection.on(LiveTranscriptionEvents.Error, on_error)

        options = LiveOptions(
            model=model,
            smart_format=True,
            language=language,
            encoding="linear16",
            sample_rate=sample_rate,
            interim_results=interim_results,
            extra=metadata,
            callback=callback,
            redact=redaction_settings,
            replace=replace_settings,
            keyterm=keyterms or None,
            endpointing=endpointing,
        )
        # TODO: pass mip_opt_out once LiveOptions supports it (SDK v5+)

        self.dg_connection.start(options)

    def handle_result(self, result):
        """Collect final results; save the utterance when Deepgram detects the end of speech."""
        if not self.save_utterance_callback:
            return
        text = result.channel.alternatives[0].transcript
        if text:
            self.heard_since_finalize = True
        if getattr(result, "is_final", False) and text:
            if self.pending_start is None:
                self.pending_start = result.start
            self.pending_end = result.start + result.duration
            self.pending_text.append(text)
        if getattr(result, "speech_final", False) or getattr(result, "from_finalize", False):
            self.flush()

    def finalize_if_quiet(self):
        """
        The participant went quiet. Meet may stop sending their audio, so Deepgram
        never hears the silence that ends the utterance; ask it to finalize now.
        """
        if not self.save_utterance_callback or not self.heard_since_finalize:
            return
        self.heard_since_finalize = False
        try:
            self.dg_connection.finalize()
        except Exception as e:
            logger.warning(f"Deepgram finalize failed: {e}")

    def flush(self):
        if not self.save_utterance_callback or not self.pending_text:
            return
        text = " ".join(self.pending_text)
        anchor = self.stream_start_time if self.stream_start_time is not None else time.time()
        duration_ms = int(round((self.pending_end - self.pending_start) * 1000))
        timestamp_ms = int(round((anchor + self.pending_start) * 1000))
        self.pending_text, self.pending_start, self.pending_end = [], None, None
        callback = self.save_utterance_callback
        _enqueue_save(lambda: callback(text, {"timestamp_ms": timestamp_ms, "duration_ms": duration_ms}))

    def send(self, data):
        if self.stream_start_time is None:
            self.stream_start_time = time.time()
        self.dg_connection.send(data)
        self.last_send_time = time.time()

    def finish(self):
        self.dg_connection.finish()
        # Speech cut off by the end of the stream still counts.
        self.flush()
