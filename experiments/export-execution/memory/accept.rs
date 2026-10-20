//! One genuine typed acceptance, without a scheduler, reservation or execution.
//! The existing destination, visibility and three scope grants are prerequisites.
mod descriptor;
use self::descriptor::Case;
use super::{descriptor::Documents, *};
use bigbrotr::{
    management::{credentials::HASH_RESERVATION, jobs::ExportCommand, sessions::SessionLifetime},
    storage::postgres::control_owner::ScopeGrantAllocation,
};
use bigbrotr_model::CommandKey;
use bigbrotr_protocol::archive_visibility::{MAX_FACT_BYTES, MAX_NATIVE_ROW_BYTES, VisibilityRead};

fn active(cancelled: &AtomicBool) -> Result<()> {
    if cancelled.load(Ordering::Acquire) {
        Err("setup cancelled; preserve actual outcome".into())
    } else {
        Ok(())
    }
}
async fn step<T, E: Debug>(
    cancelled: &AtomicBool,
    future: impl Future<Output = std::result::Result<T, E>>,
) -> Result<T> {
    active(cancelled)?;
    let value = bounded(future).await?;
    active(cancelled)?;
    Ok(value)
}

pub(crate) async fn run(path: &Path) -> Result<()> {
    let case = Case::read(path)?;
    let budget = case.limits.budget()?;
    let fixture = grant(&budget, FIXTURE_BYTES)?;
    let documents = Documents::read_pair(&case.config, &case.policy)?;
    let observer = Observer::start(&case.ledger, &budget)?;
    let cancelled = Arc::new(AtomicBool::new(false));
    let signal_flag = cancelled.clone();
    let signal = tokio::spawn(async move {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            term.recv().await;
            signal_flag.store(true, Ordering::Release);
        }
    });
    let mut held = Held::default();
    let mut visibility = None;
    let outcome = work(
        &case,
        &documents,
        &budget,
        &observer,
        &cancelled,
        &mut held,
        &mut visibility,
    )
    .await;
    let cleanup = held.close(outcome.is_ok(), &observer).await;
    // The native allocation observation/grant also outlives the Control join.
    drop(visibility);
    signal.abort();
    let _ = signal.await;
    let reported = observer.mark(
        "accept-setup-returned",
        serde_json::json!({
            "error":outcome.as_ref().err(), "cleanup_error":cleanup.as_ref().err(),
            "scope":"acceptance-only", "event_presence":"not-inferred-from-acceptance"
        }),
    );
    drop(documents);
    drop(fixture);
    let released = budget.snapshot().active_permits() == 0;
    let sampled = observer.mark(
        "grants-released",
        serde_json::json!({"all_released":released}),
    );
    let joined = observer.finish();
    outcome?;
    cleanup?;
    reported?;
    sampled?;
    joined?;
    if !released {
        return Err("setup grants remain; external exclusion required".into());
    }
    emit(
        serde_json::json!({"event":"export-accept-setup-complete","name":case.name,
        "status":"COMPLETE","state":"accepted","reservation":"not-performed",
        "execution":"not-performed","event_presence":"not-inferred-from-acceptance"}),
    )
}

async fn work(
    case: &Case,
    documents: &Documents,
    budget: &ResourceBudget,
    observation: &Observer,
    cancelled: &Arc<AtomicBool>,
    held: &mut Held,
    retained_visibility: &mut Option<bigbrotr::storage::archive::ArchivedVisibility>,
) -> Result<()> {
    let control_bytes = case
        .control
        .settings()?
        .preparation_input_reservation_bytes() as u64
        * 8
        + 528 * MIB;
    let catalog_bytes = case
        .catalog
        .settings()?
        .preparation_input_reservation_bytes() as u64
        * 8
        + 80 * MIB;
    let overlap = 64 * MAX_NATIVE_ROW_BYTES as u64;
    let native_bytes = case
        .limits
        .engine_memory_bytes
        .checked_add(case.limits.native_reserve_bytes)
        .ok_or("native arithmetic")?;
    {
        // Conservative peak includes hash and reader even though they run serially.
        let _data = budget
            .try_acquire(
                ResourceRequest::data(
                    control_bytes + catalog_bytes + MIB + HASH_RESERVATION + overlap,
                    6,
                    native_bytes,
                )
                .map_err(error)?,
            )
            .map_err(error)?;
        let _control = budget
            .try_acquire(ResourceRequest::control(128 * 1024).map_err(error)?)
            .map_err(error)?;
    }
    observation.mark("accept-setup-admitted", serde_json::json!({
        "command_key":case.command_key,"command":case.command,"control_service_bytes":control_bytes,
        "catalog_manager_bytes":catalog_bytes,"reader_bytes":MIB,"hash_bytes":HASH_RESERVATION,
        "visibility_parent_bytes":overlap,"native_bytes":native_bytes,
        "native_control_bytes":128*1024,"fixture_bytes":FIXTURE_BYTES,"job_cap_qualification":false
    }))?;
    active(cancelled)?;
    case.installation()
        .verify(Instant::now() + Duration::from_secs(5), cancelled)
        .map_err(error)?;
    let path = case.destination.clone();
    let dataset = case.dataset;
    let destination_id = case.command.destination.id;
    let (destination, checked) = tokio::task::spawn_blocking(move || {
        let destination =
            Arc::new(LocalDestination::open(&path, dataset, destination_id).map_err(error)?);
        let checked = destination.clone().check_for_acceptance().map_err(error)?;
        Ok::<_, String>((destination, checked))
    })
    .await
    .map_err(|_| "destination checker panicked")??;
    held.destination = Some(destination);
    held.checked_destination = Some(checked);
    active(cancelled)?;
    let (client, driver, _, permit) = connect(&case.control, budget, 512 * MIB).await?;
    held.control_grant = Some(permit);
    held.control_driver = Some(tokio::spawn(driver));
    observation.mark("control-connected", backend(&client).await?)?;
    active(cancelled)?;
    let incarnation = fresh()?.try_into().map_err(error)?;
    let acquired = bounded(ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: case.dataset,
            instance: case.instance,
            authority_mode: "managed".into(),
            role_instance_id: fresh()?,
            incarnation,
            lease_ttl: Duration::from_secs(5),
        },
        |text, digest| documents.validate(text, digest),
        &Cell::new(false),
    ))
    .await?;
    // Store the returned owner before any fallible observation/cancellation check.
    held.owner = Some(acquired.0);
    let owner = held.owner.as_mut().ok_or("missing Control")?;
    observation.mark(
        "control-acquired",
        serde_json::json!({
            "epoch":owner.identity().epoch.get(),"incarnation":incarnation
        }),
    )?;
    step(cancelled, owner.require_browser_sessions()).await?;
    step(cancelled, owner.require_export_jobs()).await?;
    step(cancelled, owner.require_archive_reader()).await?;
    step(cancelled, owner.require_export_execution()).await?; // typed read-back prerequisites only
    let selected = step(
        cancelled,
        owner.read_archive_selection(
            case.command.allocation.catalog_id,
            case.command.allocation.frontier_revision.get(),
        ),
    )
    .await?
    .ok_or("selected Archive frontier absent")?;
    let identity = CatalogIdentity::local(
        case.dataset,
        case.command.allocation.catalog_id,
        &case.data_root,
    )
    .map_err(error)?;
    held.coordinator = Some(LocalCoordinator::open(&identity, &case.state_root).map_err(error)?);
    let coordinator = held.coordinator.as_ref().ok_or("missing coordinator")?;
    step(cancelled, owner.require_archive_coordinator(coordinator)).await?;
    let (manager, driver, route, permit) = connect(&case.catalog, budget, 64 * MIB).await?;
    held.catalog_grant = Some(permit);
    held.manager = Some(manager);
    held.catalog_driver = Some(tokio::spawn(
        coordinator.retain_connection(driver).map_err(error)?,
    ));
    let manager = held.manager.as_mut().ok_or("missing catalog")?;
    observation.mark("catalog-connected", backend(manager).await?)?;
    step(
        cancelled,
        catalog_guard::inspect_reader(manager, &identity, &case.reader_user),
    )
    .await?;

    step(
        cancelled,
        owner.refresh(|text, digest| documents.validate(text, digest)),
    )
    .await?;
    let challenge = step(
        cancelled,
        owner.begin_browser_login(&case.username, "127.0.0.1".parse().map_err(error)?),
    )
    .await?;
    let mut hashing = grant(budget, HASH_RESERVATION)?;
    let password = SecretFile::parse(&case.password_ref)
        .map_err(error)?
        .read(4096)
        .map_err(error)?;
    // Never abandon a blocking verifier; its real join owns the hashing grant.
    let verified =
        tokio::task::spawn_blocking(move || challenge.verify(password.as_bytes(), &mut hashing))
            .await
            .map_err(|_| "login verifier panicked")?
            .map_err(error)?;
    step(
        cancelled,
        owner.refresh(|text, digest| documents.validate(text, digest)),
    )
    .await?;
    let session = step(
        cancelled,
        owner.finish_browser_login(verified, SessionLifetime::default()),
    )
    .await?;
    observation.mark("browser-authenticated",serde_json::json!({
        "principal":session.observation.principal,"credential_revision":session.observation.credential_revision,
        "grant_revision":session.observation.grant_revision
    }))?;

    let capture = step(
        cancelled,
        CatalogCapture::at_snapshot(
            manager,
            coordinator,
            selected.snapshot_id,
            Duration::from_secs(30),
        ),
    )
    .await?;
    let pin = step(
        cancelled,
        owner.register_archive_pin(
            &capture,
            &ArchivePinRequest {
                lease_id: fresh()?,
                frontier_revision: selected.frontier_revision,
                ttl: Duration::from_secs(30),
            },
            &Cell::new(false),
        ),
    )
    .await?;
    observation.mark("captured-and-pinned",serde_json::json!({
        "snapshot_id":capture.snapshot_id(),"frontier_revision":selected.frontier_revision,"lease_id":pin.lease_id
    }))?;
    let expected = VisibilityRead {
        dataset: selected.dataset_id,
        scope: case.command.query.scope,
        topology_generation: selected.topology_generation,
        bounds: selected.bounds,
        snapshot_id: selected.snapshot_id,
    };
    let installation = case.installation();
    let scratch = case.scratch.clone();
    let mut limits = case.limits.preparation();
    limits.max_encoded_bytes = MAX_FACT_BYTES;
    let reader_grant = grant(budget, MIB)?;
    let password = SecretFile::parse(&case.reader_password_ref)
        .map_err(error)?
        .read(4096)
        .map_err(error)?;
    let database = case.catalog.database.clone();
    let user = case.reader_user.clone();
    let custody = coordinator.reader_custody().map_err(error)?;
    let native_budget = budget.clone();
    let cancel = cancelled.clone();
    step(
        cancelled,
        owner.refresh(|text, digest| documents.validate(text, digest)),
    )
    .await?;
    let deadline = Instant::now() + Duration::from_millis(case.limits.query_ms);
    *retained_visibility = Some(
        tokio::task::spawn_blocking(move || {
            let _reader_grant = reader_grant;
            Preparation {
                installation: &installation,
                temp_root: &scratch,
                limits,
                deadline,
                cancelled: &cancel,
            }
            .read_visibility(
                expected,
                custody,
                &CatalogRoute {
                    address: route.address(),
                    port: route.port(),
                    database: &database,
                    tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
                },
                CatalogReaderCredentials {
                    login: &user,
                    password: std::str::from_utf8(password.as_bytes()).map_err(error)?,
                },
                &native_budget,
            )
            .map_err(error)
        })
        .await
        .map_err(|_| "visibility caller panicked; custody unknown")??,
    );
    let visibility = retained_visibility
        .as_ref()
        .ok_or("missing visibility observation")?;
    active(cancelled)?;
    observation.mark(
        "visibility-observed",
        serde_json::json!({
            "read":visibility.read(),"fact":visibility.fact_ref(),"event_presence":"not-observed"
        }),
    )?;
    step(
        cancelled,
        owner.refresh(|text, digest| documents.validate(text, digest)),
    )
    .await?;
    let commit = Cell::new(false);
    let result = bounded(owner.accept_export(
        &session.session,
        &session.csrf,
        case.command_key,
        &case.command,
        Some(&ScopeGrantAllocation {
            capture: &capture,
            visibility,
            pin: &pin,
        }),
        held.checked_destination.as_ref(),
        &commit,
    ))
    .await;
    let reported = observation.mark(
        "accept-returned",
        serde_json::json!({
            "commit_started":commit.get(),"error":result.as_ref().err(),
            "receipt":result.as_ref().ok()
        }),
    );
    let receipt = result?;
    reported?;
    active(cancelled)?;
    if !receipt.created || !commit.get() {
        return Err("fresh setup command already existed".into());
    }
    let retry_commit = Cell::new(false);
    let retry = step(
        cancelled,
        owner.accept_export(
            &session.session,
            &session.csrf,
            case.command_key,
            &case.command,
            None,
            None,
            &retry_commit,
        ),
    )
    .await?;
    if retry.created
        || retry_commit.get()
        || retry.job_id != receipt.job_id
        || retry.accepted_at_us != receipt.accepted_at_us
        || retry.command_key != receipt.command_key
    {
        return Err("acceptance retry differs".into());
    }
    let accepted = step(cancelled, owner.read_export_job(receipt.job_id))
        .await?
        .ok_or("accepted job missing")?;
    let selection = accepted.selection();
    let read = visibility.read();
    if accepted.state() != ExportState::Accepted
        || accepted.revision() != 1
        || accepted.reservation().is_some()
        || selection.job() != receipt.job_id
        || selection.principal() != session.observation.principal
        || selection.catalog() != case.command.allocation.catalog_id
        || selection.frontier_revision() != case.command.allocation.frontier_revision.get()
        || selection.destination() != case.command.destination
        || selection.query().event_id.to_string() != descriptor::EVENT_ID
        || selection.query().dataset != read.dataset
        || selection.query().scope != read.scope
        || selection.query().topology_generation != read.topology_generation
        || selection.query().snapshot_id != read.snapshot_id
        || selection.query().bounds != read.bounds
    {
        return Err("setup job is not the exact unreserved Accepted selection".into());
    }
    observation.mark("accepted-job",serde_json::json!({
        "receipt":receipt,"retry":retry,"state":"accepted","reservation":null,
        "query":accepted.selection().query(),"catalog":accepted.selection().catalog(),
        "frontier_revision":accepted.selection().frontier_revision(),"revision":accepted.revision()
    }))?;
    // Only the temporary native reader pin ends. The job's distinct export pin survives.
    step(
        cancelled,
        owner.release_archive_pin(&pin, &Cell::new(false)),
    )
    .await?;
    step(cancelled, capture.finish()).await?;
    step(
        cancelled,
        owner.revoke_browser_session(&session.session, &session.csrf),
    )
    .await?;
    observation.mark(
        "accept-setup-reader-released",
        serde_json::json!({"export_pin":"retained"}),
    )?;
    Ok(())
}
