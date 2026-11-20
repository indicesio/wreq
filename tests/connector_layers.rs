mod support;

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::future::join_all;
use pretty_env_logger::env_logger;
use support::{layer::DelayLayer, server};
use tower::{
    layer::util::Identity, limit::ConcurrencyLimitLayer, timeout::TimeoutLayer,
    util::MapResponseLayer,
};
use wreq::{Client, ClientBuilder, Conn};

#[tokio::test]
async fn non_op_layer() {
    let _ = env_logger::try_init();

    let server = server::http(move |_req| async { http::Response::default() });

    let url = format!("http://{}", server.addr());

    let client = Client::builder()
        .connector_layer(Identity::new())
        .no_proxy()
        .build()
        .unwrap();

    let res = client.get(url).send().await;

    assert!(res.is_ok());
}

#[tokio::test]
async fn non_op_layer_with_timeout() {
    let _ = env_logger::try_init();

    let server = server::http(move |_req| async { http::Response::default() });

    let client = Client::builder()
        .connector_layer(Identity::new())
        .connector_layer(DelayLayer::new(Duration::from_secs(60)))
        .connect_timeout(Duration::from_millis(200))
        .no_proxy()
        .build()
        .unwrap();

    let url = format!("http://{}", server.addr());

    let res = client.get(url).send().await;

    let err = res.unwrap_err();

    assert!(err.is_connect() && err.is_timeout());
}

#[tokio::test]
async fn with_connect_timeout_layer_never_returning() {
    let _ = env_logger::try_init();

    let server = server::http(move |_req| async { http::Response::default() });

    let client = Client::builder()
        .connector_layer(DelayLayer::new(Duration::from_secs(60)))
        .connector_layer(TimeoutLayer::new(Duration::from_millis(100)))
        .no_proxy()
        .build()
        .unwrap();

    let url = format!("http://{}", server.addr());

    let res = client.get(url).send().await;

    let err = res.unwrap_err();

    assert!(err.is_connect() && err.is_timeout());
}

#[tokio::test]
async fn with_connect_timeout_layer_slow() {
    let _ = env_logger::try_init();

    let server = server::http(move |_req| async { http::Response::default() });

    let url = format!("http://{}", server.addr());

    let client = Client::builder()
        .connector_layer(DelayLayer::new(Duration::from_millis(200)))
        .connector_layer(TimeoutLayer::new(Duration::from_millis(100)))
        .no_proxy()
        .build()
        .unwrap();

    let res = client.get(url).send().await;

    let err = res.unwrap_err();

    assert!(err.is_connect() && err.is_timeout());
}

#[tokio::test]
async fn multiple_timeout_layers_under_threshold() {
    let _ = env_logger::try_init();

    let server = server::http(move |_req| async { http::Response::default() });

    let url = format!("http://{}", server.addr());

    let client = Client::builder()
        .connector_layer(DelayLayer::new(Duration::from_millis(100)))
        .connector_layer(TimeoutLayer::new(Duration::from_millis(200)))
        .connector_layer(TimeoutLayer::new(Duration::from_millis(300)))
        .connector_layer(TimeoutLayer::new(Duration::from_millis(500)))
        .connect_timeout(Duration::from_millis(200))
        .no_proxy()
        .build()
        .unwrap();

    let res = client.get(url).send().await;

    assert!(res.is_ok());
}

#[tokio::test]
async fn multiple_timeout_layers_over_threshold() {
    let _ = env_logger::try_init();

    let server = server::http(move |_req| async { http::Response::default() });

    let url = format!("http://{}", server.addr());

    let client = Client::builder()
        .connector_layer(DelayLayer::new(Duration::from_millis(100)))
        .connector_layer(TimeoutLayer::new(Duration::from_millis(50)))
        .connector_layer(TimeoutLayer::new(Duration::from_millis(50)))
        .connector_layer(TimeoutLayer::new(Duration::from_millis(50)))
        .connect_timeout(Duration::from_millis(50))
        .no_proxy()
        .build()
        .unwrap();

    let res = client.get(url).send().await;

    let err = res.unwrap_err();

    assert!(err.is_connect() && err.is_timeout());
}

#[tokio::test]
async fn with_concurrency_limit_layer_timeout() {
    let _ = env_logger::try_init();

    let server = server::http(move |_req| async { http::Response::default() });

    let url = format!("http://{}", server.addr());

    let client = Client::builder()
        .connector_layer(DelayLayer::new(Duration::from_millis(100)))
        .connector_layer(ConcurrencyLimitLayer::new(1))
        .timeout(Duration::from_millis(200))
        .pool_max_idle_per_host(0) // disable connection reuse to force resource contention on the concurrency limit semaphore
        .no_proxy()
        .build()
        .unwrap();

    // first call succeeds since no resource contention
    let res = client.get(url.clone()).send().await;
    assert!(res.is_ok());

    // 3 calls where the second two wait on the first and time out
    let mut futures = Vec::new();
    for _ in 0..3 {
        futures.push(client.clone().get(url.clone()).send());
    }

    let all_res = join_all(futures).await;

    let timed_out = all_res
        .into_iter()
        .any(|res| res.is_err_and(|err| err.is_timeout()));

    assert!(timed_out, "at least one request should have timed out");
}

#[tokio::test]
async fn with_concurrency_limit_layer_success() {
    let _ = env_logger::try_init();

    let server = server::http(move |_req| async { http::Response::default() });

    let url = format!("http://{}", server.addr());

    let client = Client::builder()
        .connector_layer(DelayLayer::new(Duration::from_millis(100)))
        .connector_layer(TimeoutLayer::new(Duration::from_millis(200)))
        .connector_layer(ConcurrencyLimitLayer::new(1))
        .timeout(Duration::from_millis(1000))
        .pool_max_idle_per_host(0) // disable connection reuse to force resource contention on the concurrency limit semaphore
        .no_proxy()
        .build()
        .unwrap();

    // first call succeeds since no resource contention
    let res = client.get(url.clone()).send().await;
    assert!(res.is_ok());

    // 3 calls of which all are individually below the inner timeout
    // and the sum is below outer timeout which affects the final call which waited the whole time
    let mut futures = Vec::new();
    for _ in 0..3 {
        futures.push(client.clone().get(url.clone()).send());
    }

    let all_res = join_all(futures).await;

    for res in all_res.into_iter() {
        assert!(
            res.is_ok(),
            "neither outer long timeout or inner short timeout should be exceeded"
        );
    }
}

#[tokio::test]
async fn no_generic_bounds_required_for_client_new() {
    let _ = env_logger::try_init();

    let server = server::http(move |_req| async { http::Response::default() });

    let url = format!("http://{}", server.addr());

    let res = wreq::get(url).send().await;

    assert!(res.is_ok());
}

#[derive(Clone, Debug, PartialEq)]
struct Id(usize);

fn add_conn_id_layer(builder: ClientBuilder) -> ClientBuilder {
    let connections = Arc::new(AtomicUsize::new(0));
    builder.connector_layer(MapResponseLayer::new(move |conn: Conn| {
        conn.extra(Id(connections.fetch_add(1, Ordering::Relaxed)))
    }))
}

async fn make_request_and_get_id(client: &Client, url: String) -> (SocketAddr, Id) {
    let res = client.get(url).send().await.unwrap();
    let id = res.extensions().get::<Id>().cloned().unwrap();
    let local_addr = res.local_addr().unwrap();
    (local_addr, id)
}

#[tokio::test]
async fn same_conn_same_extra() {
    let _ = env_logger::try_init();

    let server = server::http(move |_req| async { http::Response::default() });
    let url = format!("http://{}", server.addr());

    let client = add_conn_id_layer(Client::builder())
        .no_proxy()
        .build()
        .unwrap();

    let (addr1, id1) = make_request_and_get_id(&client, url.clone()).await;
    let (addr2, id2) = make_request_and_get_id(&client, url).await;
    assert_eq!(addr1, addr2, "Connection not reused");
    assert_eq!(id1, id2, "Connection reused but ids are different");
}

#[tokio::test]
async fn diff_conn_diff_extra() {
    let _ = env_logger::try_init();

    let server1 = server::http(move |_req| async { http::Response::default() });
    let url1 = format!("http://{}", server1.addr());
    let server2 = server::http(move |_req| async { http::Response::default() });
    let url2 = format!("http://{}", server2.addr());

    let client = add_conn_id_layer(Client::builder())
        .no_proxy()
        .build()
        .unwrap();

    let (addr1, id1) = make_request_and_get_id(&client, url1).await;
    let (addr2, id2) = make_request_and_get_id(&client, url2).await;
    assert_ne!(addr1, addr2, "Connection reused");
    assert_ne!(id1, id2, "Connection not reused but ids are same");
}
