//! End-to-end tests of the frontend against a mocked origin.

use axum::Router;
use axum::body::{Body, to_bytes};
use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, Uri};
use nestor::{BlockSize, CacheConfig, Consistency, Nestor};
use nestor_s3::sigv4::{Credentials, SigningKeys};
use nestor_s3::{Addressing, Auth, OriginConfig, S3Config, S3Service};
use tower::ServiceExt;
use wiremock::matchers::{header_exists, method, path, query_param};
use wiremock::{Mock, MockServer, Request as MockRequest, Respond, ResponseTemplate};

const OBJECT: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
const ETAG: &str = "\"etag-1\"";
const LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";

struct ObjectResponder(Bytes);

impl Respond for ObjectResponder {
    fn respond(&self, request: &MockRequest) -> ResponseTemplate {
        let base = |status: u16| {
            ResponseTemplate::new(status)
                .append_header("ETag", ETAG)
                .append_header("Last-Modified", LAST_MODIFIED)
                .append_header("Accept-Ranges", "bytes")
        };
        let range = request
            .headers
            .get("range")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("bytes="))
            .and_then(|v| {
                let (a, b) = v.split_once('-')?;
                let start: usize = a.parse().ok()?;
                let end: usize = b.parse().ok()?;
                Some(start..(end + 1).min(self.0.len()))
            });
        match range {
            Some(r) if request.method == Method::GET => base(206)
                .append_header(
                    "Content-Range",
                    format!("bytes {}-{}/{}", r.start, r.end - 1, self.0.len()),
                )
                .set_body_bytes(self.0.slice(r)),
            _ if request.method == Method::HEAD => {
                base(200).append_header("Content-Length", self.0.len().to_string())
            }
            _ => base(200).set_body_bytes(self.0.clone()),
        }
    }
}

async fn setup(auth: Auth, populate: Option<usize>) -> (MockServer, Router, Nestor) {
    let origin = MockServer::start().await;
    let nestor = Nestor::builder(CacheConfig::memory(8 * 1024 * 1024))
        .build()
        .await
        .unwrap();
    let endpoint: Uri = origin.uri().parse().unwrap();
    let config = S3Config {
        origin: OriginConfig::anonymous(endpoint, "us-east-1").with_static_credentials(
            "origin-ak",
            "origin-sk",
            None,
        ),
        origins: None,
        auth,
        addressing: Addressing::Path,
        buckets: nestor::NamespaceConfig::default()
            .block_size(BlockSize::new(nestor::MIN_BLOCK_SIZE).unwrap())
            .consistency(Consistency::Immutable)
            .readahead(0)
            .hedge(None),
        populate_max: populate,
    };
    let service = S3Service::new(nestor.clone(), config);
    (origin, service.router(), nestor)
}

async fn call(router: &Router, req: Request<Body>) -> (Response<Body>, Bytes) {
    let response = router.clone().oneshot(req).await.unwrap();
    let (parts, body) = response.into_parts();
    let bytes = to_bytes(body, usize::MAX).await.unwrap();
    (Response::from_parts(parts, Body::empty()), bytes)
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header("host", "localhost")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn get_is_served_from_cache_after_first_miss() {
    let (origin, router, _) = setup(Auth::Anonymous, None).await;
    Mock::given(path("/data/obj.bin"))
        .respond_with(ObjectResponder(Bytes::from_static(OBJECT)))
        .expect(1)
        .mount(&origin)
        .await;

    for _ in 0..3 {
        let (resp, body) = call(&router, get("/data/obj.bin")).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["etag"], ETAG);
        assert_eq!(resp.headers()["content-length"], OBJECT.len().to_string());
        assert_eq!(resp.headers()["accept-ranges"], "bytes");
        assert_eq!(&body[..], OBJECT);
    }
}

#[tokio::test]
async fn range_requests_return_partial_content() {
    let (origin, router, _) = setup(Auth::Anonymous, None).await;
    Mock::given(path("/data/obj.bin"))
        .respond_with(ObjectResponder(Bytes::from_static(OBJECT)))
        .mount(&origin)
        .await;

    let mut req = get("/data/obj.bin");
    req.headers_mut()
        .insert("range", "bytes=10-19".parse().unwrap());
    let (resp, body) = call(&router, req).await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.headers()["content-range"], "bytes 10-19/36");
    assert_eq!(resp.headers()["content-length"], "10");
    assert_eq!(&body[..], &OBJECT[10..20]);

    let mut req = get("/data/obj.bin");
    req.headers_mut()
        .insert("range", "bytes=-6".parse().unwrap());
    let (resp, body) = call(&router, req).await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(&body[..], &OBJECT[30..]);

    let mut req = get("/data/obj.bin");
    req.headers_mut()
        .insert("range", "bytes=100-".parse().unwrap());
    let (resp, _) = call(&router, req).await;
    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(resp.headers()["content-range"], "bytes */36");
}

#[tokio::test]
async fn head_and_conditional_requests() {
    let (origin, router, _) = setup(Auth::Anonymous, None).await;
    Mock::given(path("/data/obj.bin"))
        .respond_with(ObjectResponder(Bytes::from_static(OBJECT)))
        .expect(1)
        .mount(&origin)
        .await;

    let mut req = get("/data/obj.bin");
    *req.method_mut() = Method::HEAD;
    let (resp, body) = call(&router, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["content-length"], "36");
    assert!(body.is_empty());

    let mut req = get("/data/obj.bin");
    req.headers_mut()
        .insert("if-none-match", ETAG.parse().unwrap());
    let (resp, _) = call(&router, req).await;
    assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);

    let mut req = get("/data/obj.bin");
    req.headers_mut()
        .insert("if-match", "\"other\"".parse().unwrap());
    let (resp, _) = call(&router, req).await;
    assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);
}

#[tokio::test]
async fn missing_object_maps_to_no_such_key() {
    let (origin, router, _) = setup(Auth::Anonymous, None).await;
    Mock::given(path("/data/missing"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&origin)
        .await;

    let (resp, body) = call(&router, get("/data/missing")).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("<Code>NoSuchKey</Code>"), "{body}");
}

#[tokio::test]
async fn list_is_forwarded_and_resigned() {
    let (origin, router, _) = setup(Auth::Anonymous, None).await;
    Mock::given(method("GET"))
        .and(path("/data/"))
        .and(query_param("list-type", "2"))
        .and(query_param("prefix", "a/b"))
        .and(header_exists("authorization"))
        .and(header_exists("x-amz-date"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<ListBucketResult/>"))
        .expect(1)
        .mount(&origin)
        .await;

    let (resp, body) = call(&router, get("/data?list-type=2&prefix=a%2Fb")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(&body[..], b"<ListBucketResult/>");
}

#[tokio::test]
async fn put_populates_cache_and_delete_invalidates() {
    let (origin, router, _) = setup(Auth::Anonymous, Some(1024 * 1024)).await;
    Mock::given(method("PUT"))
        .and(path("/data/new.bin"))
        .respond_with(ResponseTemplate::new(200).append_header("ETag", "\"put-etag\""))
        .expect(1)
        .mount(&origin)
        .await;
    Mock::given(method("GET"))
        .and(path("/data/new.bin"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&origin)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/data/new.bin"))
        .respond_with(ResponseTemplate::new(404))
        .expect(0)
        .mount(&origin)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/data/new.bin"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&origin)
        .await;

    let put = Request::builder()
        .method(Method::PUT)
        .uri("/data/new.bin")
        .header("host", "localhost")
        .header("content-length", OBJECT.len().to_string())
        .body(Body::from(OBJECT))
        .unwrap();
    let (resp, _) = call(&router, put).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["etag"], "\"put-etag\"");

    let (resp, body) = call(&router, get("/data/new.bin")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["etag"], "\"put-etag\"");
    assert_eq!(&body[..], OBJECT);

    let delete = Request::builder()
        .method(Method::DELETE)
        .uri("/data/new.bin")
        .header("host", "localhost")
        .body(Body::empty())
        .unwrap();
    let (resp, _) = call(&router, delete).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let (resp, _) = call(&router, get("/data/new.bin")).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn static_auth_rejects_unsigned_and_accepts_signed() {
    let auth = Auth::Static {
        access_key: "client-ak".into(),
        secret_key: "client-sk".into(),
    };
    let (origin, router, _) = setup(auth, None).await;
    Mock::given(path("/data/obj.bin"))
        .respond_with(ObjectResponder(Bytes::from_static(OBJECT)))
        .mount(&origin)
        .await;

    let (resp, body) = call(&router, get("/data/obj.bin")).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert!(String::from_utf8_lossy(&body).contains("AccessDenied"));

    let mut signed = get("/data/obj.bin");
    let uri = signed.uri().clone();
    SigningKeys::default().sign(
        &Method::GET,
        &uri,
        signed.headers_mut(),
        Credentials {
            access_key: "client-ak",
            secret_key: "client-sk",
            session_token: None,
        },
        "us-east-1",
        chrono::Utc::now(),
    );
    let (resp, body) = call(&router, signed).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(&body[..], OBJECT);
}

#[tokio::test]
async fn health_endpoint() {
    let (_origin, router, _) = setup(Auth::Anonymous, None).await;
    let (resp, body) = call(&router, get("/-/health")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(&body[..], b"ok");
}
