use bigbrotr_archive_worker::catalog::{BootstrapRequest, CatalogError};

#[test]
fn bootstrap_wire_is_bounded_closed_and_keeps_credentials_out_of_errors() {
    let request=BootstrapRequest::read(br#"{"dsn":"host=postgres dbname=lake user=bootstrap password=canary","data_root":"/archive/"}"#.as_slice()).unwrap();
    assert_eq!(request.data_root(), "/archive/");
    for input in [
        br#"{"dsn":"password=canary","data_root":"/archive/","sql":"DROP DATABASE"}"#.as_slice(),
        br#"{"dsn":"","data_root":"/archive/"}"#.as_slice(),
        br#"{"dsn":"password=canary","data_root":"s3://bucket/root/"}"#.as_slice(),
        br#"{"dsn":"password=canary\n","data_root":"/archive/"}"#.as_slice(),
        br#"{"dsn":"password=canary","data_root":"/archive/../other/"}"#.as_slice(),
        br#"{"dsn":"password=canary","data_root":"/archive/*/"}"#.as_slice(),
        br#"{"dsn":"password=canary","data_root":"/archive/?/"}"#.as_slice(),
        br#"{"dsn":"password=canary","data_root":"/archive/[x]/"}"#.as_slice(),
    ] {
        let error = BootstrapRequest::read(input).err().unwrap();
        assert_eq!(error, CatalogError::InvalidInput);
        assert!(!error.to_string().contains("canary"));
    }
    assert_eq!(
        BootstrapRequest::read(vec![b'x'; 32769].as_slice())
            .err()
            .unwrap(),
        CatalogError::InvalidInput
    );
}
