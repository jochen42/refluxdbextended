use crate::server::TestServer;
use influxdb3_client::Error;
use influxdb3_client::Precision;

// This fork removes the upstream resource limits (5 databases, 2000 tables,
// 500 columns per table). Verify writes beyond those old limits succeed.
#[tokio::test]
async fn limits() -> Result<(), Error> {
    let server = TestServer::spawn().await;

    // More than 5 databases must be accepted
    for db in ["one", "two", "three", "four", "five", "six"] {
        server
            .write_lp_to_db(
                db,
                "cpu,host=s1,region=us-east usage=0.9 2998574938\n",
                Precision::Nanosecond,
            )
            .await?;
    }

    // A row wider than the old 500-column limit must be accepted
    let mut lp_501 = String::from("cpu,host=foo,region=bar usage=2");
    for i in 5..=501 {
        lp_501.push_str(&format!(",column{i}=1"));
    }
    lp_501.push_str(" 2998574938\n");

    server
        .write_lp_to_db("two", &lp_501, Precision::Second)
        .await?;

    Ok(())
}
