# Meeting webhook

Labs › Meeting webhook (off by default) sends each finished meeting to an
address you control, such as a Zapier "Catch Hook", an n8n webhook, or a
local server, so transcripts can be processed without polling.

```sh
hex labs on meeting-webhook
hex settings set labs.meeting_webhook_url https://hooks.zapier.com/hooks/catch/123/abc/
```

Or turn it on in Settings › Labs and type the address in the field under
its row. Only `https://` addresses are accepted, plus `http://` to
`localhost`, `127.0.0.1`, or `[::1]`.

## When it sends

```ts
Stop & Transcribe (or the next launch finishing a meeting Hex quit during)
└── final transcript published, Markdown exported
    └── POST event "meeting.transcribed", notes_pending: true | false
        └── notes_pending: true -> notes pass (Apple Intelligence or OpenCode)
            ├── written -> POST "meeting.notes"         (heading and notes now in the Markdown)
            └── failed  -> POST "meeting.notes_failed"  with notes_error
Regenerate summary in the Meetings pane -> another "meeting.notes" / "meeting.notes_failed"
```

A recording that fails, or is interrupted by quitting, sends nothing until
its final transcript is published.

## Delivery

- `POST` with `Content-Type: application/json` and user agent
  `Hex/<version> meeting-webhook`, from one background thread, in order.
- Redirects are not followed. Each attempt has 30 seconds.
- A network error, `429`, or `5xx` is retried after 5 s and again after
  30 s. Any other non-2xx answer ends that event.
- At most 16 events wait to be sent; more are dropped. Outcomes are logged
  to `process.log` (`meeting webhook sent`, `… refused the event`,
  `… gave up`); the window shows nothing.
- There is no signature. Treat the address as a secret, and use an
  unguessable path.

## Body

The same shape as `hex meetings show <id> --json`
([transcript-cli.md](transcript-cli.md)), with these additions:

```ts
type MeetingWebhook = {
  version: 1
  event: "meeting.transcribed" | "meeting.notes" | "meeting.notes_failed"
  sent_at_ms: number
  notes_pending?: boolean // meeting.transcribed only: whether a notes event follows
  notes_error?: string // meeting.notes_failed only
  meeting: {
    id: string
    title: string
    timestamp_ms: number
    duration_ms: number
    application: string | null // detected call app or calendar call provider
    status: "complete"
    in_progress: boolean
    transcript_source: "final"
    warnings: string[]
    created_at_ms: number
    ended_at_ms: number | null
    title_source: "default" | "call" | "calendar" | "user"
    calendar: {
      title: string
      attendees: string[] // the other attendees' names
      event_id?: string // the calendar's event identifier (EventKit on macOS)
      start_ms?: number // scheduled start and end, Unix milliseconds
      end_ms?: number
      link?: string // the call link in the event
    } | null // only for a meeting recorded from a calendar reminder, or a detected call matched to one
    markdown_path: string | null // the exported Markdown file
  }
  notes: { title: string; summary: string; generated_at_ms: number } | null
  chapters?: { number: number; title: string; summary: string; start_ms: number; end_ms: number }[]
  entries: {
    source: "microphone" | "system"
    speaker: "You" | "Them" // remote speakers aren't separated
    start_ms: number // relative to the meeting start
    end_ms: number
    text: string
  }[]
  markdown: string | null // the exported file's text, notes included once written
}
```

Calendar `event_id`, times, and `link` are kept only for meetings recorded
with this version or later; older meetings have `title` and `attendees` only.
Linux and Windows read no calendar, so `calendar` is `null` there.
