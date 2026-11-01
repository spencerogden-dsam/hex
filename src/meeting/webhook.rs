//! Labs › Meeting webhook: a finished meeting goes to the user's own
//! address as JSON, once when its final transcript is published and again
//! when its notes pass ends (or fails).
//!
//! One worker thread sends events in order from a bounded queue, so a slow
//! or unreachable webhook never holds up the meeting controller. Each POST is
//! `curl` with the address on its stdin config and the body in an owner-only
//! file, so neither shows up in another process's argument list. Redirects
//! are not followed. A network failure, 429, or 5xx is retried twice; other
//! answers end the attempt. Results are logged, never shown.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread;
use std::time::Duration;

use color_eyre::eyre::{Result, WrapErr, eyre};
use serde_json::Value;

use super::MeetingManifest;
use super::query::{Library, OffsetRange};

/// Payload version; bump on an incompatible change.
const VERSION: u32 = 1;
/// Events waiting to be sent; more are dropped and logged.
const QUEUE: usize = 16;
/// Waits before the second and third attempts.
const RETRY_AFTER: [Duration; 2] = [Duration::from_secs(5), Duration::from_secs(30)];
const MAX_TIME_SECS: &str = "30";
const CONNECT_TIMEOUT_SECS: &str = "10";

/// What happened to the meeting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    /// The final transcript was published. `notes_pending` says whether a
    /// notes event will follow.
    Transcribed { notes_pending: bool },
    /// Notes (title and summary) were written into the meeting and its
    /// Markdown.
    Notes,
    /// The notes pass ended without notes.
    NotesFailed { error: String },
}

impl Event {
    fn name(&self) -> &'static str {
        match self {
            Self::Transcribed { .. } => "meeting.transcribed",
            Self::Notes => "meeting.notes",
            Self::NotesFailed { .. } => "meeting.notes_failed",
        }
    }
}

struct Job {
    id: String,
    event: Event,
}

/// Queues `event` for meeting `id` when Labs › Meeting webhook is on and an
/// address is saved. Never blocks.
pub fn send(id: &str, event: Event) {
    if !crate::labs::meeting_webhook() {
        return;
    }
    if crate::app_settings::AppSettings::saved_labs()
        .meeting_webhook_url
        .is_none_or(|url| url.trim().is_empty())
    {
        tracing::info!(meeting = id, "meeting webhook is on but has no address");
        return;
    }
    let job = Job {
        id: id.into(),
        event,
    };
    let Some(queue) = worker() else { return };
    match queue.try_send(job) {
        Ok(()) => {}
        Err(TrySendError::Full(job)) => {
            tracing::warn!(
                meeting = job.id,
                event = job.event.name(),
                "meeting webhook queue is full; dropped"
            );
        }
        Err(TrySendError::Disconnected(_)) => {}
    }
}

fn worker() -> Option<&'static SyncSender<Job>> {
    static QUEUE_SENDER: OnceLock<Option<SyncSender<Job>>> = OnceLock::new();
    QUEUE_SENDER
        .get_or_init(|| {
            let (sender, receiver) = mpsc::sync_channel::<Job>(QUEUE);
            thread::Builder::new()
                .name("meeting-webhook".into())
                .spawn(move || {
                    for job in receiver {
                        if crate::desktop::process_exit::quitting() {
                            return;
                        }
                        deliver(&job);
                    }
                })
                .map_err(|error| tracing::error!(%error, "could not start the meeting webhook"))
                .ok()
                .map(|_| sender)
        })
        .as_ref()
}

fn deliver(job: &Job) {
    // Read at send time, so a changed or cleared address applies at once.
    let Some(url) = crate::app_settings::AppSettings::saved_labs()
        .meeting_webhook_url
        .filter(|url| !url.trim().is_empty())
    else {
        return;
    };
    let url = match normalized(url.trim()) {
        Ok(url) => url,
        Err(why) => {
            tracing::warn!(meeting = job.id, why, "meeting webhook address refused");
            return;
        }
    };
    let url = url.as_str();
    let body = match payload(&job.id, &job.event) {
        Ok(body) => body,
        Err(error) => {
            tracing::warn!(%error, meeting = job.id, "could not build the meeting webhook payload");
            return;
        }
    };
    let event = job.event.name();
    for attempt in 0..=RETRY_AFTER.len() {
        if attempt > 0 {
            thread::sleep(RETRY_AFTER[attempt - 1]);
            if crate::desktop::process_exit::quitting() {
                return;
            }
        }
        match post(url, &body) {
            Ok(status) if (200..300).contains(&status) => {
                tracing::info!(meeting = job.id, event, status, "meeting webhook sent");
                return;
            }
            Ok(status) if status == 429 || status >= 500 => {
                tracing::warn!(
                    meeting = job.id,
                    event,
                    status,
                    attempt,
                    "meeting webhook answered; retrying"
                );
            }
            Ok(status) => {
                tracing::warn!(
                    meeting = job.id,
                    event,
                    status,
                    "meeting webhook refused the event"
                );
                return;
            }
            Err(error) => {
                tracing::warn!(%error, meeting = job.id, event, attempt, "meeting webhook unreachable");
            }
        }
    }
    tracing::warn!(meeting = job.id, event, "meeting webhook gave up");
}

/// Whether `url` may receive meetings: https anywhere, or http only to this
/// computer (a local automation server). No spaces or control characters.
pub fn check_url(url: &str) -> std::result::Result<(), String> {
    normalized(url).map(drop)
}

/// `url` as sent: parsed and percent-encoded, once it passes `check_url`.
fn normalized(url: &str) -> std::result::Result<String, String> {
    if url.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err("the address can't contain spaces or line breaks".into());
    }
    let parsed = url::Url::parse(url).map_err(|error| format!("not a web address: {error}"))?;
    if parsed.host_str().is_none_or(str::is_empty) {
        return Err("the address needs a host".into());
    }
    match parsed.scheme() {
        "https" => Ok(parsed.into()),
        "http" if is_loopback(&parsed) => Ok(parsed.into()),
        "http" => Err("use https; http is allowed only to localhost".into()),
        _ => Err("expected an https:// address".into()),
    }
}

fn is_loopback(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

/// The JSON for `event`: `hex meetings show --json`'s shape (meeting,
/// notes, entries, chapters) plus the event, the calendar event, and the
/// Markdown file's text.
fn payload(id: &str, event: &Event) -> Result<Vec<u8>> {
    let library = Library::new(super::root()?);
    let meeting = library.meeting(id)?;
    let now = crate::desktop::events::now_ms();
    let json = crate::control::transcript_cli::meeting_json(&meeting, OffsetRange::default(), now)?;
    let markdown = super::transcript_markdown_for_manifest(&meeting.manifest).ok();
    serde_json::to_vec(&decorate(json, &meeting.manifest, event, markdown, now))
        .wrap_err("could not encode the meeting webhook payload")
}

fn decorate(
    mut json: Value,
    manifest: &MeetingManifest,
    event: &Event,
    markdown: Option<String>,
    now: u64,
) -> Value {
    json["version"] = VERSION.into();
    json["event"] = event.name().into();
    json["sent_at_ms"] = now.into();
    match event {
        Event::Transcribed { notes_pending } => json["notes_pending"] = (*notes_pending).into(),
        Event::NotesFailed { error } => json["notes_error"] = error.as_str().into(),
        Event::Notes => {}
    }
    if let Some(meeting) = json.get_mut("meeting").and_then(Value::as_object_mut) {
        meeting.insert("created_at_ms".into(), manifest.created_at_ms.into());
        meeting.insert("ended_at_ms".into(), manifest.ended_at_ms.into());
        meeting.insert(
            "title_source".into(),
            serde_json::to_value(manifest.title_source()).unwrap_or(Value::Null),
        );
        meeting.insert(
            "calendar".into(),
            serde_json::to_value(&manifest.calendar).unwrap_or(Value::Null),
        );
        meeting.insert(
            "markdown_path".into(),
            manifest.export_path.as_ref().map_or(Value::Null, |path| {
                path.to_string_lossy().into_owned().into()
            }),
        );
    }
    if let Some(entries) = json.get_mut("entries").and_then(Value::as_array_mut) {
        for entry in entries.iter_mut().filter_map(Value::as_object_mut) {
            let speaker = match entry.get("source").and_then(Value::as_str) {
                Some("microphone") => "You",
                Some("system") => "Them",
                _ => continue,
            };
            entry.insert("speaker".into(), speaker.into());
        }
    }
    json["markdown"] = markdown.map_or(Value::Null, Value::from);
    json
}

/// POSTs `body` to `url`; the HTTP status (0 when curl got no answer).
fn post(url: &str, body: &[u8]) -> Result<u16> {
    let file = BodyFile::write(body)?;
    let protocol = if url.starts_with("https:") {
        "=https"
    } else {
        "=http"
    };
    let mut child = crate::desktop::background_process::command(if cfg!(windows) {
        "curl.exe"
    } else {
        "curl"
    })
    .args([
        "--silent",
        "--show-error",
        "--proto",
        protocol,
        "--connect-timeout",
        CONNECT_TIMEOUT_SECS,
        "--max-time",
        MAX_TIME_SECS,
        "--user-agent",
        concat!("Hex/", env!("CARGO_PKG_VERSION"), " meeting-webhook"),
        "--header",
        "Content-Type: application/json",
        "--output",
        if cfg!(windows) { "NUL" } else { "/dev/null" },
        "--write-out",
        "%{http_code}",
        "--config",
        "-",
    ])
    .arg("--data-binary")
    .arg({
        let mut at = std::ffi::OsString::from("@");
        at.push(file.path());
        at
    })
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .wrap_err("could not run curl")?;
    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| eyre!("curl has no stdin"))?;
        stdin.write_all(curl_config(url).as_bytes())?;
    }
    let output = child.wait_with_output()?;
    let status = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u16>()
        .unwrap_or(0);
    if status == 0 {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(eyre!("curl: {}", stderr.trim()));
    }
    Ok(status)
}

/// A curl config line naming `url`, quoted for curl's config parser.
fn curl_config(url: &str) -> String {
    let quoted = url.replace('\\', "\\\\").replace('"', "\\\"");
    format!("url = \"{quoted}\"\n")
}

/// The request body on disk, owner-only, removed when sent.
struct BodyFile(PathBuf);

impl BodyFile {
    fn write(body: &[u8]) -> Result<Self> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        // Created exclusively, so an existing file or link is never followed.
        let path = std::env::temp_dir().join(format!(
            "hex-meeting-webhook-{}-{}-{}.json",
            std::process::id(),
            crate::desktop::events::now_ms(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&path)
            .wrap_err_with(|| format!("could not write {}", path.display()))?;
        file.write_all(body)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for BodyFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_are_https_or_local_http() {
        assert!(check_url("https://hooks.zapier.com/hooks/catch/1/abc/").is_ok());
        assert!(check_url("http://localhost:5678/webhook/hex").is_ok());
        assert!(check_url("http://127.0.0.1:8080/x").is_ok());
        assert!(check_url("http://[::1]/x").is_ok());
        assert!(check_url("http://example.com/hook").is_err());
        assert!(check_url("ftp://example.com/hook").is_err());
        assert!(check_url("https://example.com/a b").is_err());
        assert!(check_url("https://example.com/\nurl = \"x\"").is_err());
        assert!(check_url("hooks.zapier.com").is_err());
        assert_eq!(
            normalized("https://h.example/x?a=\"b").as_deref(),
            Ok("https://h.example/x?a=%22b")
        );
    }

    #[test]
    fn curl_posts_the_body_to_the_address_and_reads_the_status() {
        use std::io::{BufRead, BufReader, Read};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a local port");
        let port = listener.local_addr().expect("its address").port();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("curl connects");
            let mut reader = BufReader::new(stream.try_clone().expect("a second handle"));
            let mut request_line = String::new();
            reader.read_line(&mut request_line).expect("a request line");
            let mut length = 0;
            let mut content_type = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).expect("a header");
                let line = line.trim_end().to_string();
                if line.is_empty() {
                    break;
                }
                let lower = line.to_ascii_lowercase();
                if let Some(value) = lower.strip_prefix("content-length:") {
                    length = value.trim().parse().expect("a length");
                }
                if lower.starts_with("content-type:") {
                    content_type = line;
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).expect("the body");
            let mut stream = stream;
            stream
                .write_all(
                    b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .expect("an answer");
            (request_line, content_type, body)
        });
        let url = format!("http://127.0.0.1:{port}/hook?token=a\"b");
        let status = post(&url, br#"{"event":"meeting.notes"}"#).expect("curl runs");
        let (request_line, content_type, body) = server.join().expect("the server");
        assert_eq!(status, 202);
        // The config quoting carries the address through unchanged.
        assert!(
            request_line.starts_with("POST /hook?token=a\"b "),
            "{request_line}"
        );
        assert_eq!(
            content_type.to_ascii_lowercase(),
            "content-type: application/json"
        );
        assert_eq!(body, br#"{"event":"meeting.notes"}"#);
    }

    #[test]
    fn the_config_line_quotes_the_address() {
        assert_eq!(
            curl_config(r#"https://h/x?a="b"\c"#),
            "url = \"https://h/x?a=\\\"b\\\"\\\\c\"\n"
        );
    }

    #[test]
    fn the_payload_names_the_event_calendar_speakers_and_markdown() {
        let mut manifest: MeetingManifest = serde_json::from_value(serde_json::json!({
            "schema_version": 1, "id": "m1", "title": "Q4 sync", "status": "complete",
            "created_at_ms": 1_000, "ended_at_ms": 61_000, "duration_ms": 60_000,
            "system": {"file": "system.wav", "samples": 0, "dropped_packets": 0},
            "microphone": {"file": "microphone.wav", "samples": 0, "dropped_packets": 0},
        }))
        .expect("a minimal manifest");
        manifest.calendar = Some(super::super::MeetingCalendar {
            title: "Q4 sync".into(),
            attendees: vec!["Ana Ruiz".into()],
            event_id: Some("EVT-1".into()),
            start_ms: Some(0),
            end_ms: Some(1_800_000),
            link: Some("https://zoom.us/j/1".into()),
        });
        let shown = serde_json::json!({
            "version": 1,
            "meeting": {"id": "m1", "title": "Q4 sync"},
            "notes": null,
            "entries": [
                {"source": "microphone", "start_ms": 0, "end_ms": 900, "text": "Hi"},
                {"source": "system", "start_ms": 1000, "end_ms": 1900, "text": "Hello"},
            ],
        });
        let json = decorate(
            shown,
            &manifest,
            &Event::Transcribed {
                notes_pending: true,
            },
            Some("# Q4 sync\n".into()),
            5,
        );
        assert_eq!(json["event"], "meeting.transcribed");
        assert_eq!(json["notes_pending"], true);
        assert_eq!(json["meeting"]["calendar"]["event_id"], "EVT-1");
        assert_eq!(json["meeting"]["calendar"]["attendees"][0], "Ana Ruiz");
        assert_eq!(json["meeting"]["ended_at_ms"], 61_000);
        assert_eq!(json["entries"][0]["speaker"], "You");
        assert_eq!(json["entries"][1]["speaker"], "Them");
        assert_eq!(json["markdown"], "# Q4 sync\n");

        let failed = decorate(
            serde_json::json!({"meeting": {}}),
            &manifest,
            &Event::NotesFailed {
                error: "no model".into(),
            },
            None,
            5,
        );
        assert_eq!(failed["event"], "meeting.notes_failed");
        assert_eq!(failed["notes_error"], "no model");
        assert!(failed["markdown"].is_null());
    }
}
