//! Read-only nomination oracle; SQL input ordinals and IDs are fixture literals.
use bigbrotr::{
    acquisition::job::AcquisitionScan,
    management::sessions::SessionError,
    runtime::{ResourceBudget, ResourceRequest},
    storage::postgres::control_owner::ControlSession,
};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::{task::JoinHandle, time::timeout};

pub async fn run(
    mut owner: ControlSession,
    driver: JoinHandle<Result<(), tokio_postgres::Error>>,
    expected: &Value,
    expected_path: &str,
    budget: &ResourceBudget,
) {
    let permit = |bytes, items| {
        budget
            .try_acquire(ResourceRequest::data(bytes, items, 0).unwrap())
            .unwrap()
    };
    let bytes = 64 * 1024;
    assert_eq!(AcquisitionScan::REQUIRED_BYTES, bytes);
    for (size, items) in [(bytes - 1, 1), (bytes, 0)] {
        assert_eq!(
            owner.nominate_acquisition(None, permit(size, items)).await,
            Err(SessionError::Capacity)
        );
        assert_eq!(budget.snapshot().active_permits(), 1);
    }
    if expected["cancel"] == true {
        println!("{}", json!({"event":"read-start"}));
        assert!(
            timeout(
                Duration::from_secs(2),
                owner.nominate_acquisition(None, permit(bytes, 1))
            )
            .await
            .is_err()
        );
        assert_eq!(
            owner.nominate_acquisition(None, permit(bytes, 1)).await,
            Err(SessionError::Unavailable)
        );
        println!("{}", json!({"event":"read-cancelled"}));
        drop(owner);
        driver.await.unwrap().unwrap();
        return;
    }
    let mut scan = None;
    for (index, step) in expected["steps"].as_array().unwrap().iter().enumerate() {
        if step["restart"] == true {
            scan = None;
        }
        let found = owner
            .nominate_acquisition(scan, permit(bytes, 1))
            .await
            .unwrap();
        assert_eq!(
            json!({"cut":found.scan.admission_cut,"after":found.scan.after_ordinal,"job":found.job}),
            step["result"]
        );
        assert_eq!(budget.snapshot().active_permits(), 1);
        scan = Some(found.scan);
        if index == 0 && expected["append"] == true {
            println!(
                "{}",
                json!({"event":"scan-cut","cut":found.scan.admission_cut})
            );
            let ready = format!("{expected_path}.continue");
            timeout(Duration::from_secs(5), async {
                while !Path::new(&ready).exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
    }
    assert!(scan.unwrap().complete());
    assert_eq!(
        owner.nominate_acquisition(scan, permit(bytes, 1)).await,
        Err(SessionError::InvalidInput)
    );
    assert_eq!(
        owner
            .nominate_acquisition(
                Some(AcquisitionScan {
                    admission_cut: 1,
                    after_ordinal: 2
                }),
                permit(bytes, 1)
            )
            .await,
        Err(SessionError::InvalidInput)
    );
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();
}
