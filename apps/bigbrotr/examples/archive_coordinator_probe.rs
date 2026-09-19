//! Native fixture: create only a local initialization reservation, or hold the
//! actual registered coordinator. No catalog initialization/publication claim.
use bigbrotr::storage::{
    archive::{LocalCoordinator, LocalInitialization},
    postgres::catalog_guard::CatalogIdentity,
};
use std::{
    io::{Read, Write},
    path::Path,
};
fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(args.len() == 3 || args.len() == 4);
    let root = Path::new(&args[2]).canonicalize().unwrap();
    let identity = CatalogIdentity::local(
        [1; 16].try_into().unwrap(),
        [0x5a; 16].try_into().unwrap(),
        &format!("{}/", root.display()),
    )
    .unwrap();
    if args[1] == "reserve" {
        assert_eq!(args.len(), 3);
        drop(LocalInitialization::create(&identity, [3; 16]).unwrap());
    } else {
        assert_eq!(args[1], "hold");
        assert_eq!(args.len(), 4);
        let _owner = LocalCoordinator::open(&identity, Path::new(&args[3])).unwrap();
        println!("held");
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_exact(&mut [0]).unwrap();
    }
}
