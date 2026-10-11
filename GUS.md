# Gus fork of Attendee

`gus` = upstream `main` + small, opt-in patches Gus needs, each also offered upstream:

- **Deepgram streaming without a callback, with keyterms and endpointing** (`deepgram-streaming-utterances`): `transcription_settings.deepgram.streaming = true` saves an utterance per participant as soon as Deepgram detects the end of speech and sends `transcript.update` at once. Gus uses it with keyterms from the company's notes.

`.github/workflows/sync-upstream.yml` rebases `gus` onto upstream every Monday and opens an issue on a conflict. When a patch is merged upstream, drop it here.
