use bigbrotr_model::{
    AccessScopeId, DatasetId, DictionaryKind, EndpointId, PolicyEpoch, QualifiedRef, SelectorId,
    acquisition::AcquisitionScope,
};

fn visibility() -> QualifiedRef {
    QualifiedRef {
        dataset_id: DatasetId::try_from([1; 16]).unwrap(),
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: [2; 16].try_into().unwrap(),
        ordinal: bigbrotr_model::Ordinal::new(1).unwrap(),
    }
}

#[test]
fn scope_keeps_all_semantic_dimensions_and_refuses_foreign_visibility() {
    let dataset = visibility().dataset_id;
    let endpoint = EndpointId::try_from([3; 16]).unwrap();
    let selector = SelectorId::try_from([4; 16]).unwrap();
    let access = AccessScopeId::try_from([5; 16]).unwrap();
    let scope = AcquisitionScope::new(
        dataset,
        endpoint,
        selector,
        access,
        visibility(),
        PolicyEpoch::new(1),
    )
    .unwrap();
    assert_eq!(scope.dataset(), dataset);
    assert_eq!(scope.endpoint(), endpoint);
    assert_eq!(scope.selector(), selector);
    assert_eq!(scope.access(), access);
    assert_eq!(scope.visibility(), visibility());
    assert_eq!(scope.policy_epoch(), PolicyEpoch::new(1));
    assert_ne!(
        scope,
        AcquisitionScope::new(
            dataset,
            endpoint,
            selector,
            access,
            visibility(),
            PolicyEpoch::new(2)
        )
        .unwrap()
    );
    assert!(
        AcquisitionScope::new(
            dataset,
            endpoint,
            selector,
            access,
            visibility(),
            PolicyEpoch::new(0)
        )
        .is_err()
    );
    let mut other = visibility();
    other.dataset_id = [9; 16].try_into().unwrap();
    assert!(
        AcquisitionScope::new(
            dataset,
            endpoint,
            selector,
            access,
            other,
            PolicyEpoch::new(1)
        )
        .is_err()
    );
    other = visibility();
    other.dictionary_kind = DictionaryKind::Endpoint;
    assert!(
        AcquisitionScope::new(
            dataset,
            endpoint,
            selector,
            access,
            other,
            PolicyEpoch::new(1)
        )
        .is_err()
    );
}
