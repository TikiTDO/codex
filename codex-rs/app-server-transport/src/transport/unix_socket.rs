//! Control socket startup, guarded rendezvous paths, and WebSocket acceptance.

#[cfg(target_os = "linux")]
use std::fs::File;
#[cfg(target_os = "linux")]
use std::fs::Metadata;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::io::Result as IoResult;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "linux")]
use std::path::Component;
use std::path::Path;

use super::TransportEvent;
use crate::transport::websocket::run_websocket_connection;
use codex_uds::UnixListener;
use codex_uds::UnixStream;
use codex_utils_absolute_path::AbsolutePathBuf;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_tungstenite::accept_async;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing::warn;

#[cfg(unix)]
const CONTROL_SOCKET_MODE: u32 = 0o600;

pub async fn start_control_socket_acceptor(
    socket_path: AbsolutePathBuf,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    shutdown_token: CancellationToken,
) -> IoResult<JoinHandle<()>> {
    let prepared = prepare_control_socket(socket_path).await?;
    start_prepared_control_socket_acceptor(prepared, transport_event_tx, shutdown_token).await
}

/// A checked socket path that retains parent identity when the platform supports it.
pub struct PreparedControlSocket {
    socket_path: AbsolutePathBuf,
    parent: PreparedControlSocketParent,
}

enum PreparedControlSocketParent {
    RevalidateBeforeBind,
    #[cfg(target_os = "linux")]
    Retained {
        _directory: File,
    },
    #[cfg(windows)]
    RetainedWindows {
        _directory: std::os::windows::io::OwnedHandle,
    },
}

/// Prepares a control socket path once and retains any identity needed by the acceptor.
pub async fn prepare_control_socket(
    socket_path: AbsolutePathBuf,
) -> IoResult<PreparedControlSocket> {
    #[cfg(target_os = "linux")]
    if let Some(prepared) = prepare_proc_fd_control_socket(socket_path.as_path()).await? {
        return Ok(prepared);
    }

    #[cfg(windows)]
    {
        if let Some(parent) = socket_path.as_path().parent() {
            codex_uds::prepare_private_socket_directory(parent).await?;
        }
        let (path, directory) = codex_uds::validate_private_socket_path(socket_path.as_path())?;
        let socket_path = AbsolutePathBuf::from_absolute_path_checked(path)?;
        prepare_control_socket_file(socket_path.as_path()).await?;
        return Ok(PreparedControlSocket {
            socket_path,
            parent: PreparedControlSocketParent::RetainedWindows {
                _directory: directory,
            },
        });
    }

    #[cfg(not(windows))]
    prepare_control_socket_path(socket_path.as_path()).await?;
    #[cfg(not(windows))]
    Ok(PreparedControlSocket {
        socket_path,
        parent: PreparedControlSocketParent::RevalidateBeforeBind,
    })
}

/// Binds a previously prepared control socket without reopening its parent coordinate.
pub async fn start_prepared_control_socket_acceptor(
    prepared: PreparedControlSocket,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    shutdown_token: CancellationToken,
) -> IoResult<JoinHandle<()>> {
    if matches!(
        &prepared.parent,
        PreparedControlSocketParent::RevalidateBeforeBind
    ) {
        prepare_control_socket_path(prepared.socket_path.as_path()).await?;
    }
    let listener = UnixListener::bind(prepared.socket_path.as_path()).await?;
    let socket_guard = ControlSocketFileGuard { prepared };
    set_control_socket_permissions(socket_guard.socket_path().as_path()).await?;
    info!(
        socket_path = %socket_guard.socket_path().display(),
        "app-server control socket listening"
    );

    Ok(tokio::spawn(run_control_socket_acceptor(
        listener,
        transport_event_tx,
        shutdown_token,
        socket_guard,
    )))
}

#[cfg(target_os = "linux")]
async fn prepare_proc_fd_control_socket(
    socket_path: &Path,
) -> IoResult<Option<PreparedControlSocket>> {
    let Some(parent) = socket_path.parent() else {
        return Ok(None);
    };
    if !is_exact_proc_fd_path(parent) {
        return Ok(None);
    }
    let Some(socket_name) = socket_path.file_name() else {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "proc-fd control socket path has no file name",
        ));
    };

    let link_metadata = std::fs::symlink_metadata(parent)?;
    if !link_metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "proc-fd control socket parent is not a descriptor link",
        ));
    }
    let followed_metadata = std::fs::metadata(parent)?;
    let directory = File::from(rustix::fs::open(
        parent,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?);
    let opened_metadata = directory.metadata()?;
    let current_uid = rustix::process::geteuid().as_raw();
    if !same_directory(&followed_metadata, &opened_metadata)
        || !opened_metadata.is_dir()
        || opened_metadata.mode() & 0o777 != 0o700
        || link_metadata.uid() != current_uid
        || opened_metadata.uid() != current_uid
    {
        return Err(std::io::Error::new(
            ErrorKind::PermissionDenied,
            "proc-fd control socket parent is not one owner-private directory",
        ));
    }

    let anchored_path = AbsolutePathBuf::from_absolute_path(
        Path::new("/proc/self/fd")
            .join(directory.as_raw_fd().to_string())
            .join(socket_name),
    )?;
    prepare_control_socket_file(anchored_path.as_path()).await?;
    Ok(Some(PreparedControlSocket {
        socket_path: anchored_path,
        parent: PreparedControlSocketParent::Retained {
            _directory: directory,
        },
    }))
}

#[cfg(target_os = "linux")]
fn is_exact_proc_fd_path(path: &Path) -> bool {
    let mut components = path.components();
    if components.next() != Some(Component::RootDir)
        || components.next() != Some(Component::Normal(std::ffi::OsStr::new("proc")))
    {
        return false;
    }
    let (
        Some(Component::Normal(pid)),
        Some(Component::Normal(fd_directory)),
        Some(Component::Normal(fd)),
    ) = (components.next(), components.next(), components.next())
    else {
        return false;
    };
    fd_directory == "fd"
        && components.next().is_none()
        && canonical_decimal(pid).is_some_and(|value| value > 0)
        && canonical_decimal(fd).is_some()
}

#[cfg(target_os = "linux")]
fn canonical_decimal(value: &std::ffi::OsStr) -> Option<u32> {
    let raw = value.to_str()?;
    let parsed = raw.parse::<u32>().ok()?;
    (parsed.to_string() == raw).then_some(parsed)
}

#[cfg(target_os = "linux")]
fn same_directory(left: &Metadata, right: &Metadata) -> bool {
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.mode() == right.mode()
        && left.uid() == right.uid()
        && left.gid() == right.gid()
}

async fn run_control_socket_acceptor(
    mut listener: UnixListener,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    shutdown_token: CancellationToken,
    socket_guard: ControlSocketFileGuard,
) {
    let _socket_guard = socket_guard;
    loop {
        let stream = tokio::select! {
            _ = shutdown_token.cancelled() => {
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok(stream) => stream,
                    Err(err) => {
                        if matches!(
                            err.kind(),
                            ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset | ErrorKind::Interrupted
                        ) {
                            warn!("recoverable control socket accept error: {err}");
                            continue;
                        }
                        error!("control socket accept error: {err}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }
        };

        let transport_event_tx = transport_event_tx.clone();
        tokio::spawn(async move {
            let websocket_stream = match accept_async(stream).await {
                Ok(websocket_stream) => websocket_stream,
                Err(err) => {
                    warn!("failed to upgrade control socket websocket connection: {err}");
                    return;
                }
            };
            let (websocket_writer, websocket_reader) = websocket_stream.split();
            run_websocket_connection(websocket_writer, websocket_reader, transport_event_tx).await;
        });
    }
    info!("control socket acceptor shutting down");
}

pub async fn prepare_control_socket_path(socket_path: &Path) -> IoResult<()> {
    if let Some(parent) = socket_path.parent() {
        codex_uds::prepare_private_socket_directory(parent).await?;
    }

    #[cfg(windows)]
    let (socket_path, _directory_guard) = codex_uds::validate_private_socket_path(socket_path)?;
    #[cfg(windows)]
    let socket_path = AbsolutePathBuf::from_absolute_path_checked(socket_path)?;
    #[cfg(windows)]
    let socket_path = socket_path.as_path();

    prepare_control_socket_file(socket_path).await
}

async fn prepare_control_socket_file(socket_path: &Path) -> IoResult<()> {
    match UnixStream::connect(socket_path).await {
        Ok(_stream) => {
            return Err(std::io::Error::new(
                ErrorKind::AddrInUse,
                format!(
                    "app-server control socket is already in use at {}",
                    socket_path.display()
                ),
            ));
        }
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) if err.kind() == ErrorKind::ConnectionRefused => {}
        Err(err) => {
            if !socket_path.exists() {
                return Ok(());
            }
            return Err(err);
        }
    }

    if !socket_path.try_exists()? {
        return Ok(());
    }

    if !codex_uds::is_stale_socket_path(socket_path).await? {
        return Err(std::io::Error::new(
            ErrorKind::AlreadyExists,
            format!(
                "app-server control socket path exists and is not a socket: {}",
                socket_path.display()
            ),
        ));
    }
    tokio::fs::remove_file(socket_path).await
}

pub struct AppServerStartupLock {
    _file: std::fs::File,
}

pub async fn acquire_app_server_startup_lock(
    startup_lock_path: AbsolutePathBuf,
) -> IoResult<AppServerStartupLock> {
    if let Some(parent) = startup_lock_path.as_path().parent() {
        codex_uds::prepare_private_socket_directory(parent).await?;
    }
    tokio::task::spawn_blocking(move || {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(startup_lock_path.as_path())?;
        file.lock()?;
        Ok(AppServerStartupLock { _file: file })
    })
    .await
    .map_err(|err| std::io::Error::other(format!("startup lock task failed: {err}")))?
}

#[cfg(unix)]
async fn set_control_socket_permissions(socket_path: &Path) -> IoResult<()> {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::set_permissions(
        socket_path,
        std::fs::Permissions::from_mode(CONTROL_SOCKET_MODE),
    )
    .await
}

#[cfg(not(unix))]
async fn set_control_socket_permissions(_socket_path: &Path) -> IoResult<()> {
    Ok(())
}

struct ControlSocketFileGuard {
    prepared: PreparedControlSocket,
}

impl ControlSocketFileGuard {
    fn socket_path(&self) -> &AbsolutePathBuf {
        &self.prepared.socket_path
    }
}

impl Drop for ControlSocketFileGuard {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_file(self.socket_path().as_path())
            && err.kind() != ErrorKind::NotFound
        {
            warn!(
                socket_path = %self.socket_path().display(),
                %err,
                "failed to remove app-server control socket file"
            );
        }
    }
}
