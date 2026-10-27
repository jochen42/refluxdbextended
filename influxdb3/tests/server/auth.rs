use arrow_flight::error::FlightError;
use arrow_util::assert_batches_sorted_eq;
use hashbrown::HashMap;
use influxdb3_client::Precision;
use influxdb3_server::all_paths;
use itertools::Itertools;
use observability_deps::tracing::info;
use reqwest::StatusCode;

use crate::server::{ConfigProvider, TestServer, collect_stream};

#[tokio::test]
async fn auth_http() {
    let server = TestServer::configure().with_auth().spawn().await;
    let token = server
        .auth_token
        .clone()
        .expect("admin token to have been present");

    let client = server.http_client();
    let base = server.client_addr();
    let write_lp_url = format!("{base}/api/v3/write_lp");
    let write_lp_params = [("db", "foo")];
    let query_sql_url = format!("{base}/api/v3/query_sql");
    let query_sql_params = [("db", "foo"), ("q", "select * from cpu")];

    assert_eq!(
        client
            .post(&write_lp_url)
            .query(&write_lp_params)
            .body("cpu,host=a val=1i 2998574937")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&write_lp_url)
            .query(&write_lp_params)
            .body("cpu,host=a val=1i 2998574937")
            .bearer_auth(token.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        client
            .post(&write_lp_url)
            .query(&write_lp_params)
            .body("cpu,host=a val=1i 2998574937")
            // support both Bearer and Token auth schemes
            .header("Authorization", format!("Token {token}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    // Malformed Header Tests
    // Test that there is an extra string after the token foo
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .header("Authorization", format!("Bearer {token} whee"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .header("Authorization", format!("bearer {token}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .header("Authorization", "Bearer")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .header("auth", format!("Bearer {token}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn http_write_basic_auth() {
    let server = TestServer::configure().with_auth().spawn().await;
    let token = server
        .auth_token
        .clone()
        .expect("admin token to have been present");

    let client = server.http_client();
    let base = server.client_addr();
    let write_lp_url = format!("{base}/write");
    let write_lp_params = [("db", "foo")];
    let write_lp_params_with_user_and_password = [("db", "foo"), ("u", "ignored"), ("p", &token)];
    assert_eq!(
        client
            .post(&write_lp_url)
            .query(&write_lp_params)
            .body("cpu,host=a val=1i 2998574937")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&write_lp_url)
            .query(&write_lp_params)
            .body("cpu,host=a val=1i 2998574937")
            .basic_auth("username", Some(token.clone()))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    // Note: this test does not use Authorization header
    assert_eq!(
        client
            .post(&write_lp_url)
            .query(&write_lp_params_with_user_and_password)
            .body("cpu,host=a val=1i 2998574937")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    // Malformed Header Tests
    let resp = client
        .get(&write_lp_url)
        .query(&write_lp_params)
        .header("Authorization", format!("Basic {token} whee"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        resp.text().await.unwrap(),
        r#"{"error": "Authorization header was malformed, the request was not in the form of 'Authorization: <auth-scheme> <token>', supported auth-schemes are Bearer, Token and Basic"}"#
    )
}

#[test_log::test(tokio::test)]
async fn auth_grpc() {
    let server = TestServer::configure().with_auth().spawn().await;
    let token = server
        .auth_token
        .clone()
        .expect("admin token to have been present");
    // Write some data to the server, this will be authorized through the HTTP API
    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9 2998574937\n\
            cpu,host=s1,region=us-east usage=0.89 2998574938\n\
            cpu,host=s1,region=us-east usage=0.85 2998574939",
            Precision::Second,
        )
        .await
        .unwrap();

    // Check that with a valid authorization header, it succeeds:
    for header in ["authorization", "Authorization"] {
        // Spin up a FlightSQL client
        let mut client = server.flight_sql_client("foo").await;

        // Set the authorization header on the client:
        client
            .add_header(header, &format!("Bearer {token}"))
            .unwrap();

        // Make the query again, this time it should work:
        let response = client
            .query("SELECT host, region, time, usage FROM cpu")
            .await
            .unwrap();
        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+------+---------+----------------------+-------+",
                "| host | region  | time                 | usage |",
                "+------+---------+----------------------+-------+",
                "| s1   | us-east | 2065-01-07T17:28:57Z | 0.9   |",
                "| s1   | us-east | 2065-01-07T17:28:58Z | 0.89  |",
                "| s1   | us-east | 2065-01-07T17:28:59Z | 0.85  |",
                "+------+---------+----------------------+-------+",
            ],
            &batches
        );
    }

    // Check that without providing an Authentication header, it gives back
    // an Unauthenticated error:
    {
        let mut client = server.flight_sql_client("foo").await;
        let error = client.query("SELECT * FROM cpu").await.unwrap_err();
        assert!(matches!(error, FlightError::Tonic(s) if s.code() == tonic::Code::Unauthenticated));
    }

    // Create some new clients that set the authorization header incorrectly to
    // ensure errors are returned:

    // Misspelled "Bearer"
    {
        let mut client = server.flight_sql_client("foo").await;
        client
            .add_header("authorization", &format!("bearer {token}"))
            .unwrap();
        let error = client.query("SELECT * FROM cpu").await.unwrap_err();
        assert!(matches!(error, FlightError::Tonic(s) if s.code() == tonic::Code::Unauthenticated));
    }

    // Invalid token, this actually gives Permission denied
    {
        let mut client = server.flight_sql_client("foo").await;
        client
            .add_header("authorization", "Bearer invalid-token")
            .unwrap();
        let error = client.query("SELECT * FROM cpu").await.unwrap_err();
        assert!(matches!(error, FlightError::Tonic(s) if s.code() == tonic::Code::Unauthenticated));
    }

    // Misspelled header key
    {
        let mut client = server.flight_sql_client("foo").await;
        client
            .add_header("auth", &format!("Bearer {token}"))
            .unwrap();
        let error = client.query("SELECT * FROM cpu").await.unwrap_err();
        assert!(matches!(error, FlightError::Tonic(s) if s.code() == tonic::Code::Unauthenticated));
    }
}

#[tokio::test]
async fn v1_password_parameter() {
    let server = TestServer::configure().with_auth().spawn().await;
    let token = server
        .auth_token
        .clone()
        .expect("admin token to have been present");

    let client = server.http_client();
    let query_url = format!("{base}/query", base = server.client_addr());
    let write_url = format!("{base}/write", base = server.client_addr());
    // Send requests without any authentication:
    assert_eq!(
        client
            .get(&query_url)
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::UNAUTHORIZED,
    );
    assert_eq!(
        client
            .get(&write_url)
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::UNAUTHORIZED,
    );

    // Ensure that an invalid token passed in the `p` parameter is still unauthorized:
    assert_eq!(
        client
            .get(&query_url)
            .query(&[("p", "not-the-token-you-were-looking-for")])
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::UNAUTHORIZED,
    );
    assert_eq!(
        client
            .get(&write_url)
            .query(&[("p", "not-the-token-you-were-looking-for")])
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::UNAUTHORIZED,
    );

    // make some writes so that the query API will work below:
    server
        .write_lp_to_db("foo", "cpu,host=a usage=0.9", Precision::Second)
        .await
        .unwrap();

    // Send request to query API with the token in the v1 `p` parameter:
    assert_eq!(
        client
            .get(&query_url)
            .query(&[
                ("p", token.as_str()),
                ("q", "SELECT * FROM cpu"),
                ("db", "foo")
            ])
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::OK,
    );
    // Send request to query API with the token in auth header:
    assert_eq!(
        client
            .get(&query_url)
            .query(&[("q", "SELECT * FROM cpu"), ("db", "foo")])
            .bearer_auth(&token)
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::OK,
    );

    let valid_write_body = "cpu,host=val usage=0.5";

    // Send request to write API with the token in the v1 `p` parameter:
    assert_eq!(
        client
            .post(&write_url)
            .query(&[("p", token.as_str()), ("db", "foo")])
            .body(valid_write_body)
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::NO_CONTENT,
    );
    // Send request to write API with the token in auth header:
    assert_eq!(
        client
            .post(&write_url)
            .bearer_auth(&token)
            .query(&[("db", "foo")])
            .body(valid_write_body)
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::NO_CONTENT,
    );
}

#[test_log::test(tokio::test)]
async fn disabled_auth_for_health_and_ping() {
    let server = TestServer::configure()
        .with_auth()
        .with_disable_authz(vec!["health".to_owned(), "ping".to_owned()])
        .spawn()
        .await;

    let client = server.http_client();
    let base = server.client_addr();

    // health v3 API works
    assert_eq!(
        client
            .get(format!("{base}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // health v1 API works
    assert_eq!(
        client
            .get(format!("{base}/api/v1/health"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // ping works
    assert_eq!(
        client
            .get(format!("{base}/ping"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // metrics does not work, it requires token
    assert_eq!(
        client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let token = server
        .auth_token
        .clone()
        .expect("admin token to have been present");

    // with token metrics endpoint works too
    assert_eq!(
        client
            .get(format!("{base}/metrics"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[test_log::test(tokio::test)]
async fn disabled_auth_test_exhaustive() {
    let allowed_resources = ["health".to_owned(), "ping".to_owned(), "metrics".to_owned()];
    let all_possible_disable_auth_resources =
        allowed_resources.iter().powerset().collect::<Vec<_>>();

    for combo in &all_possible_disable_auth_resources {
        let resources_disabled: Vec<String> = combo.iter().map(|r| r.to_string()).collect();
        let mut all_paths = HashMap::new();
        all_paths.insert(
            "health",
            vec![all_paths::API_V3_HEALTH, all_paths::API_V1_HEALTH],
        );
        all_paths.insert("ping", vec![all_paths::API_PING]);
        all_paths.insert("metrics", vec![all_paths::API_METRICS]);

        let server = TestServer::configure()
            .with_auth()
            .with_disable_authz(resources_disabled.clone())
            .spawn()
            .await;

        let client = server.http_client();
        let base = server.client_addr();

        // all disabled resources should be accessible
        for resource in &resources_disabled {
            let paths = all_paths
                .get(resource.as_str())
                .expect("resource to be mapped");
            for path in paths {
                assert_eq!(
                    client
                        .get(format!("{base}{path}"))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    StatusCode::OK
                );
            }
        }

        // and anything that is not disabled should not be accessible
        let resources_restricted = allowed_resources
            .iter()
            .filter(|path| !resources_disabled.contains(path))
            .map(|path| path.to_owned())
            .collect::<Vec<String>>();

        let token = server
            .auth_token
            .clone()
            .expect("admin token to have been present");

        for resource in &resources_restricted {
            let paths = all_paths
                .get(resource.as_str())
                .expect("resource to be mapped");

            for path in paths {
                // needs token to access
                assert_eq!(
                    client
                        .get(format!("{base}{path}"))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    StatusCode::UNAUTHORIZED
                );

                // with token it's fine to access
                assert_eq!(
                    client
                        .get(format!("{base}{path}"))
                        .bearer_auth(&token)
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    StatusCode::OK
                );
            }
        }
    }

    // just useful to know what combos were used
    info!(
        ?all_possible_disable_auth_resources,
        "finished running the tests for"
    );
}

/// Create a scoped token through the HTTP API and return its secret.
async fn create_scoped_token(
    server: &TestServer,
    admin_token: &str,
    name: &str,
    permissions: serde_json::Value,
) -> String {
    let resp = server
        .http_client()
        .post(format!("{}/api/v3/configure/token", server.client_addr()))
        .bearer_auth(admin_token)
        .json(&serde_json::json!({
            "token_name": name,
            "permissions": permissions,
            "expiry_secs": null,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create token {name}");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["token"]
        .as_str()
        .expect("token in response")
        .to_string()
}

#[tokio::test]
async fn read_only_token_is_enforced() {
    let server = TestServer::configure().with_auth().spawn().await;
    let admin = server.auth_token.clone().expect("admin token");
    let client = server.http_client();
    let base = server.client_addr();

    for db in ["foo", "bar"] {
        server
            .write_lp_to_db(db, "cpu,host=a usage=0.5 2998574937", Precision::Second)
            .await
            .unwrap();
    }

    let ro_foo = create_scoped_token(
        &server,
        &admin,
        "ro-foo",
        serde_json::json!([{ "resource_type": "db", "resource_names": ["foo"], "actions": ["read"] }]),
    )
    .await;
    let ro_all = create_scoped_token(
        &server,
        &admin,
        "ro-all",
        serde_json::json!([{ "resource_type": "db", "resource_names": ["*"], "actions": ["read"] }]),
    )
    .await;
    let rw_foo = create_scoped_token(
        &server,
        &admin,
        "rw-foo",
        serde_json::json!([{ "resource_type": "db", "resource_names": ["foo"], "actions": ["read", "write"] }]),
    )
    .await;

    let write = |token: String, db: &'static str| {
        client
            .post(format!("{base}/api/v3/write_lp"))
            .query(&[("db", db)])
            .body("cpu,host=b usage=0.7 2998574938")
            .bearer_auth(token)
            .send()
    };
    let query = |token: String, db: &'static str| {
        client
            .get(format!("{base}/api/v3/query_sql"))
            .query(&[("db", db), ("q", "select host from cpu")])
            .bearer_auth(token)
            .send()
    };
    let status = |r: reqwest::Result<reqwest::Response>| r.unwrap().status();

    // Read-only on foo: can read foo, nothing else.
    assert_eq!(status(query(ro_foo.clone(), "foo").await), StatusCode::OK);
    assert_eq!(
        status(query(ro_foo.clone(), "bar").await),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        status(write(ro_foo.clone(), "foo").await),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        status(write(ro_foo.clone(), "newdb").await),
        StatusCode::FORBIDDEN
    );

    // Wildcard read: reads every database, still cannot write.
    assert_eq!(status(query(ro_all.clone(), "foo").await), StatusCode::OK);
    assert_eq!(status(query(ro_all.clone(), "bar").await), StatusCode::OK);
    assert_eq!(
        status(write(ro_all.clone(), "foo").await),
        StatusCode::FORBIDDEN
    );

    // Read-write on foo: writes foo, not bar, and cannot auto-create databases.
    assert_eq!(
        status(write(rw_foo.clone(), "foo").await),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        status(write(rw_foo.clone(), "bar").await),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        status(write(rw_foo.clone(), "newdb").await),
        StatusCode::FORBIDDEN
    );

    // Legacy write endpoints are covered too.
    let v2 = client
        .post(format!("{base}/api/v2/write"))
        .query(&[("bucket", "foo")])
        .body("cpu,host=c usage=0.1 2998574939000000000")
        .bearer_auth(&ro_foo)
        .send()
        .await
        .unwrap();
    // The v2 API reports authorization failures as 401, matching InfluxDB v2.
    assert_eq!(v2.status(), StatusCode::UNAUTHORIZED);
    let v1 = client
        .post(format!("{base}/write"))
        .query(&[("db", "foo")])
        .body("cpu,host=c usage=0.1 2998574939000000000")
        .bearer_auth(&ro_foo)
        .send()
        .await
        .unwrap();
    assert!(
        matches!(
            v1.status(),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
        ),
        "v1 write: {}",
        v1.status()
    );

    // InfluxQL on v3 resolves the database before authorizing.
    let influxql = |token: &str, db: &'static str| {
        client
            .get(format!("{base}/api/v3/query_influxql"))
            .query(&[("db", db), ("q", "SELECT host FROM cpu")])
            .bearer_auth(token.to_string())
            .send()
    };
    assert_eq!(status(influxql(&ro_foo, "foo").await), StatusCode::OK);
    assert_eq!(
        status(influxql(&ro_foo, "bar").await),
        StatusCode::FORBIDDEN
    );

    // Listing databases needs a wildcard read grant.
    let list_dbs = |token: &str| {
        client
            .get(format!("{base}/api/v3/configure/database"))
            .query(&[("format", "json")])
            .bearer_auth(token.to_string())
            .send()
    };
    assert_eq!(status(list_dbs(&ro_foo).await), StatusCode::FORBIDDEN);
    assert_eq!(status(list_dbs(&ro_all).await), StatusCode::OK);

    // Operator endpoints are admin-only.
    let create_db = client
        .post(format!("{base}/api/v3/configure/database"))
        .json(&serde_json::json!({ "db": "sneaky" }))
        .bearer_auth(&ro_all)
        .send()
        .await
        .unwrap();
    assert_eq!(create_db.status(), StatusCode::FORBIDDEN);
    let delete_db = client
        .delete(format!("{base}/api/v3/configure/database"))
        .query(&[("db", "foo")])
        .bearer_auth(&rw_foo)
        .send()
        .await
        .unwrap();
    assert_eq!(delete_db.status(), StatusCode::FORBIDDEN);
    let mint = client
        .post(format!("{base}/api/v3/configure/token"))
        .bearer_auth(&ro_all)
        .json(&serde_json::json!({
            "token_name": "escalate",
            "permissions": [{ "resource_type": "db", "resource_names": ["*"], "actions": ["write"] }],
            "expiry_secs": null,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(mint.status(), StatusCode::FORBIDDEN);
    let delete_token = client
        .delete(format!("{base}/api/v3/configure/token"))
        .query(&[("token_name", "rw-foo")])
        .bearer_auth(&ro_all)
        .send()
        .await
        .unwrap();
    assert_eq!(delete_token.status(), StatusCode::FORBIDDEN);

    // Health and ping stay open to any valid token.
    for path in ["/health", "/ping"] {
        let r = client
            .get(format!("{base}{path}"))
            .bearer_auth(&ro_foo)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK, "{path}");
    }

    // Admin is unaffected.
    assert_eq!(
        status(write(admin.clone(), "bar").await),
        StatusCode::NO_CONTENT
    );
    assert_eq!(status(query(admin.clone(), "bar").await), StatusCode::OK);
}

#[tokio::test]
async fn read_only_token_v1_query_and_flight() {
    let server = TestServer::configure().with_auth().spawn().await;
    let admin = server.auth_token.clone().expect("admin token");
    let client = server.http_client();
    let base = server.client_addr();

    for db in ["foo", "bar"] {
        server
            .write_lp_to_db(db, "cpu,host=a usage=0.5 2998574937", Precision::Second)
            .await
            .unwrap();
    }
    let ro_foo = create_scoped_token(
        &server,
        &admin,
        "ro-foo",
        serde_json::json!([{ "resource_type": "db", "resource_names": ["foo"], "actions": ["read"] }]),
    )
    .await;

    // v1 /query: readable db works, other db is refused.
    let v1 = |db: &'static str, q: &'static str| {
        client
            .get(format!("{base}/query"))
            .query(&[("db", db), ("q", q)])
            .bearer_auth(&ro_foo)
            .send()
    };
    let ok = v1("foo", "SELECT host FROM cpu").await.unwrap();
    assert_eq!(ok.status(), StatusCode::OK);
    let denied = v1("bar", "SELECT host FROM cpu").await.unwrap();
    assert_ne!(
        denied.status(),
        StatusCode::OK,
        "v1 query on bar must be refused"
    );

    // SHOW DATABASES over v1 lists only what the token can read.
    let show = v1("foo", "SHOW DATABASES").await.unwrap();
    assert_eq!(show.status(), StatusCode::OK);
    let body = show.text().await.unwrap();
    assert!(body.contains("foo"), "{body}");
    assert!(!body.contains("bar"), "{body}");

    // Flight: query on foo works, bar is refused.
    let mut foo_client = server.flight_sql_client("foo").await;
    foo_client
        .add_header("authorization", &format!("Bearer {ro_foo}"))
        .unwrap();
    let response = foo_client.query("SELECT host FROM cpu").await.unwrap();
    let batches = collect_stream(response).await;
    assert!(!batches.is_empty());

    let mut bar_client = server.flight_sql_client("bar").await;
    bar_client
        .add_header("authorization", &format!("Bearer {ro_foo}"))
        .unwrap();
    let err = bar_client
        .query("SELECT host FROM cpu")
        .await
        .expect_err("flight query on bar must be refused");
    assert!(
        format!("{err:?}").contains("PermissionDenied"),
        "unexpected error: {err:?}"
    );
}
