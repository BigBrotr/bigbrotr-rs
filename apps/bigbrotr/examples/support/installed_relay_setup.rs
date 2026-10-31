//! Isolated fixture preparation through the real internal registry adapter.
//! This is deliberately not an installed endpoint-registration command.
use super::*;

pub async fn run(config: Config) {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(connection);
    let row = client
        .query_one(
            "SELECT dataset_id::bytea, instance_id::bytea FROM control.datasets",
            &[],
        )
        .await
        .unwrap();
    let id = |column| {
        bigbrotr_model::DatasetId::try_from(
            <[u8; 16]>::try_from(row.get::<_, &[u8]>(column)).unwrap(),
        )
        .unwrap()
    };
    let (mut owner, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: id(0),
            instance: id(1),
            authority_mode: "managed".into(),
            role_instance_id: [121; 16],
            incarnation: [122; 16].try_into().unwrap(),
            lease_ttl: Duration::from_secs(30),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    let endpoint =
        bigbrotr_protocol::endpoint::Endpoint::parse("ws://127.0.0.1:7447/installed-fixture")
            .unwrap();
    let receipt = owner
        .register_relay_endpoint(&endpoint, &Cell::new(false))
        .await
        .unwrap();
    assert!(receipt.created);
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();
    emit(
        serde_json::json!({"event":"installed-relay-fixture", "endpoint_id":receipt.identity,
        "url":endpoint.as_str(), "authority":"not-granted"}),
    );
}
