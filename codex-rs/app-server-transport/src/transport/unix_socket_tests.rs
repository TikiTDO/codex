use super::AppServerTransport;
use super::CHANNEL_CAPACITY;
use super::TransportEvent;
use super::acquire_app_server_startup_lock;
use super::app_server_control_socket_path;
#[cfg(target_os = "linux")]
use super::prepare_control_socket;
use super::start_control_socket_acceptor;
#[cfg(target_os = "linux")]
use super::start_prepared_control_socket_acceptor;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCNotification;
use codex_core::config::find_codex_home;
use codex_uds::UnixStream;
use codex_utils_absolute_path::AbsolutePathBuf;
use futures::SinkExt;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use std::io::Result as IoResult;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::client_async;
use tokio_tungstenite::tungstenite::Bytes;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use tokio_util::sync::CancellationToken;

#[test]
fn listen_unix_socket_parses_as_unix_socket_transport() {
    assert_eq!(
        AppServerTransport::from_listen_url("unix://"),
        Ok(AppServerTransport::UnixSocket {
            socket_path: default_control_socket_path()
        })
    );
}

#[test]
fn listen_unix_socket_accepts_absolute_custom_path() {
    assert_eq!(
        AppServerTransport::from_listen_url("unix:///tmp/codex.sock"),
        Ok(AppServerTransport::UnixSocket {
            socket_path: absolute_path("/tmp/codex.sock")
        })
    );
}

#[test]
fn listen_unix_socket_accepts_relative_custom_path() {
    assert_eq!(
        AppServerTransport::from_listen_url("unix://codex.sock"),
        Ok(AppServerTransport::UnixSocket {
            socket_path: AbsolutePathBuf::relative_to_current_dir("codex.sock")
                .expect("relative path should resolve")
        })
    );
}

#[tokio::test]
async fn control_socket_acceptor_upgrades_and_forwards_websocket_text_messages_and_pings() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let socket_path = test_socket_path(temp_dir.path());
    let (transport_event_tx, mut transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown_token = CancellationToken::new();
    let accept_handle = start_control_socket_acceptor(
        socket_path.clone(),
        transport_event_tx,
        shutdown_token.clone(),
    )
    .await
    .expect("control socket acceptor should start");

    let stream = connect_to_socket(socket_path.as_path())
        .await
        .expect("client should connect");
    let (mut websocket, response) = client_async("ws://localhost/rpc", stream)
        .await
        .expect("websocket upgrade should complete");
    assert_eq!(response.status().as_u16(), 101);

    let opened = timeout(Duration::from_secs(1), transport_event_rx.recv())
        .await
        .expect("connection opened event should arrive")
        .expect("connection opened event");
    let connection_id = match opened {
        TransportEvent::ConnectionOpened { connection_id, .. } => connection_id,
        _ => panic!("expected connection opened event"),
    };

    let notification = JSONRPCMessage::Notification(JSONRPCNotification {
        method: "initialized".to_string(),
        params: None,
    });
    websocket
        .send(WebSocketMessage::Text(
            serde_json::to_string(&notification)
                .expect("notification should serialize")
                .into(),
        ))
        .await
        .expect("notification should send");

    let incoming = timeout(Duration::from_secs(1), transport_event_rx.recv())
        .await
        .expect("incoming message event should arrive")
        .expect("incoming message event");
    assert_eq!(
        match incoming {
            TransportEvent::IncomingMessage {
                connection_id: incoming_connection_id,
                message,
            } => (incoming_connection_id, message),
            _ => panic!("expected incoming message event"),
        },
        (connection_id, notification)
    );

    websocket
        .send(WebSocketMessage::Ping(Bytes::from_static(b"check")))
        .await
        .expect("ping should send");
    let pong = timeout(Duration::from_secs(1), websocket.next())
        .await
        .expect("pong should arrive")
        .expect("pong frame")
        .expect("pong should be valid");
    assert_eq!(pong, WebSocketMessage::Pong(Bytes::from_static(b"check")));

    websocket.close(None).await.expect("close should send");
    let closed = timeout(Duration::from_secs(1), transport_event_rx.recv())
        .await
        .expect("connection closed event should arrive")
        .expect("connection closed event");
    assert!(matches!(
        closed,
        TransportEvent::ConnectionClosed {
            connection_id: closed_connection_id,
        } if closed_connection_id == connection_id
    ));

    shutdown_token.cancel();
    accept_handle.await.expect("acceptor should join");
    assert_socket_path_removed(socket_path.as_path());
}

#[tokio::test]
async fn app_server_startup_lock_serializes_waiters() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let lock_path = test_startup_lock_path(temp_dir.path());
    let first_lock = acquire_app_server_startup_lock(lock_path.clone())
        .await
        .expect("first startup lock should succeed");
    let mut second_lock = tokio::spawn(acquire_app_server_startup_lock(lock_path));

    assert!(
        timeout(Duration::from_millis(100), &mut second_lock)
            .await
            .is_err()
    );

    drop(first_lock);
    second_lock
        .await
        .expect("second startup lock task should join")
        .expect("second startup lock should succeed");
}

#[cfg(unix)]
#[tokio::test]
async fn control_socket_file_is_private_after_bind() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let socket_path = test_socket_path(temp_dir.path());
    let (transport_event_tx, _transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown_token = CancellationToken::new();
    let accept_handle = start_control_socket_acceptor(
        socket_path.clone(),
        transport_event_tx,
        shutdown_token.clone(),
    )
    .await
    .expect("control socket acceptor should start");

    let metadata = tokio::fs::metadata(socket_path.as_path())
        .await
        .expect("socket metadata should exist");
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);

    shutdown_token.cancel();
    accept_handle.await.expect("acceptor should join");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn proc_fd_preparation_pins_the_directory_through_bind_and_cleanup() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path().join("socket-root");
    std::fs::create_dir(&root).expect("socket root should be created");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("socket root should be private");
    let external_directory = std::fs::File::open(&root).expect("socket root should open");
    let external_socket_path = AbsolutePathBuf::from_absolute_path(
        Path::new("/proc")
            .join(std::process::id().to_string())
            .join("fd")
            .join(external_directory.as_raw_fd().to_string())
            .join("app-server.sock"),
    )
    .expect("proc-fd socket path should resolve lexically");
    let prepared = prepare_control_socket(external_socket_path)
        .await
        .expect("proc-fd socket should prepare");

    let moved = temp_dir.path().join("socket-root-moved");
    std::fs::rename(&root, &moved).expect("socket root should move");
    std::fs::create_dir(&root).expect("replacement root should be created");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("replacement root should be private");
    let sentinel = root.join("sentinel");
    std::fs::write(&sentinel, b"unchanged").expect("replacement sentinel should be written");
    drop(external_directory);

    let (transport_event_tx, _transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown_token = CancellationToken::new();
    let accept_handle = start_prepared_control_socket_acceptor(
        prepared,
        transport_event_tx,
        shutdown_token.clone(),
    )
    .await
    .expect("prepared proc-fd socket should bind");

    assert_eq!(
        (
            moved.join("app-server.sock").exists(),
            root.join("app-server.sock").exists(),
            std::fs::read(&sentinel).expect("replacement sentinel should remain readable"),
        ),
        (true, false, b"unchanged".to_vec()),
    );

    shutdown_token.cancel();
    accept_handle.await.expect("acceptor should join");
    assert_eq!(
        (
            moved.join("app-server.sock").exists(),
            root.join("app-server.sock").exists(),
            std::fs::read(&sentinel).expect("replacement sentinel should remain readable"),
        ),
        (false, false, b"unchanged".to_vec()),
    );
}

#[cfg(unix)]
#[tokio::test]
async fn ordinary_prepared_socket_revalidates_its_parent_before_bind() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path().join("socket-root");
    std::fs::create_dir(&root).expect("socket root should be created");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("socket root should be private");
    let socket_path = AbsolutePathBuf::from_absolute_path(root.join("app-server.sock"))
        .expect("ordinary socket path should be absolute");
    let prepared = prepare_control_socket(socket_path)
        .await
        .expect("ordinary socket should prepare");

    let moved = temp_dir.path().join("socket-root-moved");
    std::fs::rename(&root, &moved).expect("socket root should move");
    let replacement = temp_dir.path().join("replacement-root");
    std::fs::create_dir(&replacement).expect("replacement root should be created");
    std::os::unix::fs::symlink(&replacement, &root)
        .expect("replacement parent symlink should be created");

    let (transport_event_tx, _transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let error = start_prepared_control_socket_acceptor(
        prepared,
        transport_event_tx,
        CancellationToken::new(),
    )
    .await
    .expect_err("replaced ordinary socket parent should be refused");

    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(
        (
            moved.join("app-server.sock").exists(),
            replacement.join("app-server.sock").exists(),
        ),
        (false, false),
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn proc_fd_preparation_refuses_non_private_and_ordinary_symlink_parents() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path().join("socket-root");
    std::fs::create_dir(&root).expect("socket root should be created");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))
        .expect("socket root mode should be set");
    let directory = std::fs::File::open(&root).expect("socket root should open");
    let proc_fd_socket = AbsolutePathBuf::from_absolute_path(
        Path::new("/proc")
            .join(std::process::id().to_string())
            .join("fd")
            .join(directory.as_raw_fd().to_string())
            .join("app-server.sock"),
    )
    .expect("proc-fd socket path should resolve lexically");
    let proc_fd_error = prepare_control_socket(proc_fd_socket)
        .await
        .err()
        .expect("non-private proc-fd parent should be refused");

    let link = temp_dir.path().join("ordinary-link");
    std::os::unix::fs::symlink(&root, &link).expect("ordinary symlink should be created");
    let linked_socket = AbsolutePathBuf::from_absolute_path(link.join("app-server.sock"))
        .expect("linked socket path should resolve lexically");
    let symlink_error = prepare_control_socket(linked_socket)
        .await
        .err()
        .expect("ordinary symlink parent should be refused");

    assert_eq!(
        (proc_fd_error.kind(), symlink_error.kind()),
        (
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::AlreadyExists
        ),
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn proc_fd_preparation_refuses_fifo_descriptor_without_blocking() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let fifo = temp_dir.path().join("socket-parent-fifo");
    rustix::fs::mkfifoat(rustix::fs::CWD, &fifo, rustix::fs::Mode::RUSR)
        .expect("fifo should be created");
    let fifo_descriptor = rustix::fs::open(
        &fifo,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .expect("fifo should open without waiting for a writer");
    let proc_fd_socket = AbsolutePathBuf::from_absolute_path(
        Path::new("/proc")
            .join(std::process::id().to_string())
            .join("fd")
            .join(fifo_descriptor.as_raw_fd().to_string())
            .join("app-server.sock"),
    )
    .expect("proc-fd socket path should resolve lexically");

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        prepare_control_socket(proc_fd_socket),
    )
    .await
    .expect("non-directory proc-fd preparation should not block");
    let error = result
        .err()
        .expect("non-directory proc-fd parent should be refused");

    assert_eq!(error.kind(), std::io::ErrorKind::NotADirectory);
}

#[cfg(windows)]
#[tokio::test]
async fn control_socket_pins_directory_until_shutdown() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let socket_path = test_socket_path(temp_dir.path());
    let directory = socket_path.as_path().parent().unwrap();
    let moved = temp_dir.path().join("moved");
    let (tx, _rx) = mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let acceptor = start_control_socket_acceptor(socket_path.clone(), tx, shutdown.clone())
        .await
        .expect("acceptor");
    assert!(std::fs::rename(directory, &moved).is_err());
    shutdown.cancel();
    acceptor.await.expect("shutdown");
    std::fs::rename(directory, moved).expect("directory unpinned after cleanup");
}

fn absolute_path(path: &str) -> AbsolutePathBuf {
    AbsolutePathBuf::from_absolute_path(path).expect("absolute path")
}

fn default_control_socket_path() -> AbsolutePathBuf {
    let codex_home = find_codex_home().expect("codex home");
    app_server_control_socket_path(&codex_home).expect("default control socket path")
}

fn test_socket_path(temp_dir: &Path) -> AbsolutePathBuf {
    AbsolutePathBuf::from_absolute_path(
        temp_dir
            .join("app-server-control")
            .join("app-server-control.sock"),
    )
    .expect("socket path should resolve")
}

fn test_startup_lock_path(temp_dir: &Path) -> AbsolutePathBuf {
    AbsolutePathBuf::from_absolute_path(
        temp_dir
            .join("app-server-control")
            .join("app-server-startup.lock"),
    )
    .expect("startup lock path should resolve")
}

async fn connect_to_socket(socket_path: &Path) -> IoResult<UnixStream> {
    UnixStream::connect(socket_path).await
}

#[cfg(unix)]
fn assert_socket_path_removed(socket_path: &Path) {
    assert!(!socket_path.exists());
}

#[cfg(windows)]
fn assert_socket_path_removed(_socket_path: &Path) {
    // uds_windows uses a regular filesystem path as its rendezvous point,
    // but there is no Unix socket filesystem node to assert on.
}
