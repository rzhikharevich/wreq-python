use std::{
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures_util::future::BoxFuture;
use http::Extensions;
use pyo3::prelude::*;
use tower_layer::Layer;
use tower_service::Service;
use wreq::Conn;

/// Durations measured for one request.
#[derive(Clone, Copy)]
#[pyclass(frozen, get_all)]
pub struct Timings {
    /// How long the request waited for the client to set up the connection. Zero if the
    /// connection was ready before the request started.
    connection_wait: Duration,
    /// How long the client took to set up the connection that delivered the response.
    connection_setup: Duration,
    /// The time from the start of the request until the response headers arrived.
    time_to_headers: Duration,
}

/// The start and end of one connection setup. wreq copies it into the extensions of every
/// response that the connection delivers.
#[derive(Clone, Copy, Debug)]
struct Connection {
    start: Instant,
    end: Instant,
}

// ===== impl Timings =====

impl Timings {
    /// The timings of a request that started at `request_start`. Call this when the response
    /// headers arrive, because `time_to_headers` ends at the call.
    pub fn new(request_start: Instant, extensions: &Extensions) -> Timings {
        let &Connection { start, end } = extensions
            .get()
            .expect("the connector layer records every connection");
        Timings {
            connection_wait: end.saturating_duration_since(request_start),
            connection_setup: end.duration_since(start),
            time_to_headers: request_start.elapsed(),
        }
    }
}

// ===== impl ConnectLayer =====

/// Measures the setup time of each connection. The setup includes name resolution, the TCP
/// handshake, the proxy handshake and the TLS handshake.
#[derive(Clone)]
pub struct ConnectLayer;

#[derive(Clone)]
pub struct Connect<S>(S);

impl<S> Layer<S> for ConnectLayer {
    type Service = Connect<S>;

    fn layer(&self, inner: S) -> Connect<S> {
        Connect(inner)
    }
}

impl<S, R> Service<R> for Connect<S>
where
    S: Service<R, Response = Conn>,
    S::Future: Send + 'static,
{
    type Response = Conn;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Conn, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        let start = Instant::now();
        let connect = self.0.call(request);
        Box::pin(async move {
            let conn = connect.await?;
            let end = Instant::now();
            Ok(conn.extra(Connection { start, end }))
        })
    }
}
