//! Native browser fixture over the actual executable. No substituted API body.
use super::*;

pub(super) struct Options {
    browser: String,
    mount: String,
    assets: PathBuf,
    script: PathBuf,
}
impl Options {
    pub fn parse(args: &mut impl Iterator<Item = String>) -> Self {
        let browser = args.next().expect("browser");
        assert!(matches!(
            browser.as_str(),
            "chromium" | "firefox" | "webkit"
        ));
        let mount = args.next().expect("mount");
        assert!(matches!(mount.as_str(), "root" | "control"));
        let assets = PathBuf::from(args.next().expect("verified bundle directory"));
        let script = PathBuf::from(args.next().expect("exact-source browser script"));
        assert!(assets.is_absolute() && script.is_absolute());
        Self {
            browser,
            mount,
            assets,
            script,
        }
    }
    pub fn database(&self) -> String {
        // Both components come from closed ASCII enums, never arbitrary SQL.
        format!("panel_{}_{}", self.browser, self.mount)
    }
    fn base(&self) -> &str {
        if self.mount == "root" {
            "/"
        } else {
            "/control/"
        }
    }
    pub fn configure(&self, document: &mut toml::Table) {
        let assets = toml::Table::from_iter([
            ("directory".into(), self.assets.to_str().unwrap().into()),
            ("base_path".into(), self.base().into()),
        ]);
        document
            .get_mut("administration")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .insert("assets".into(), assets.into());
    }
}
struct BrowserProcess(Child);
impl Drop for BrowserProcess {
    fn drop(&mut self) {
        // This joins only Node. Playwright starts separate browser process
        // groups; the native driver must collect the dedicated container even
        // on panic/timeout, and must never reuse it for another browser case.
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

pub(super) async fn run(
    binary: &Path,
    config: &Path,
    home: &Path,
    admin: &Client,
    principal: &str,
    options: Options,
) {
    let mut server = Server::start(binary, config, home, "panel-initial");
    let first = server.ready();
    assert_eq!(
        first["listeners"][0]["private_assets"]["base_path"],
        options.base()
    );
    let mut browser = BrowserProcess(
        Command::new("node")
            .arg(&options.script)
            .arg(&options.browser)
            .arg(options.base())
            .arg(home)
            .arg(principal)
            .stdout(Stdio::from(
                File::create_new(home.join("browser.stdout")).unwrap(),
            ))
            .stderr(Stdio::from(
                File::create_new(home.join("browser.stderr")).unwrap(),
            ))
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut command = 1;
    let mut boundary = None;
    loop {
        let request = home.join(format!("browser-command-{command}.json"));
        if request.exists() {
            assert!(fs::metadata(&request).unwrap().len() <= 1024);
            let value: Value = serde_json::from_slice(&fs::read(&request).unwrap()).unwrap();
            match command {
                1 => {
                    assert_eq!(value, json!({"command":"observe-boundary"}));
                    boundary = Some((session_state(admin).await, bucket_state(admin).await));
                }
                2 => {
                    assert_eq!(value, json!({"command":"verify-boundary"}));
                    assert!(
                        boundary.take().unwrap()
                            == (session_state(admin).await, bucket_state(admin).await),
                        "denied requests changed durable session or bucket rows"
                    );
                }
                3 => {
                    assert_eq!(value, json!({"command":"restart"}));
                    let before = session_state(admin).await;
                    server.stop();
                    server = Server::start(binary, config, home, "panel-restart");
                    let after = server.ready();
                    assert_ne!(first["owner_epoch"], after["owner_epoch"]);
                    assert!(
                        before == session_state(admin).await,
                        "restart changed session rows"
                    );
                }
                _ => panic!("unexpected browser fixture command"),
            }
            let temporary = home.join(format!("browser-response-{command}.tmp"));
            fs::write(&temporary, b"{\"state\":\"verified\"}\n").unwrap();
            fs::rename(
                temporary,
                home.join(format!("browser-response-{command}.json")),
            )
            .unwrap();
            command += 1;
        }
        if let Some(status) = browser.0.try_wait().unwrap() {
            assert!(
                status.success(),
                "actual browser fixture failed; inspect protected lab output"
            );
            break;
        }
        assert!(Instant::now() < deadline, "native browser deadline");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        command, 4,
        "browser did not execute required SQL controls/restart"
    );
    let report: Value =
        serde_json::from_slice(&fs::read(home.join("browser-report.json")).unwrap()).unwrap();
    assert_eq!(report["status"], "PASS");
    assert_eq!(report["browser"], options.browser);
    assert_eq!(report["base"], options.base());
    assert_eq!(report["cases"].as_array().unwrap().len(), 8);
    // Two explicit successful logins, one wrong password and two confirmed
    // logouts. Revoked rows are removed only by a subsequent admitted login.
    assert_eq!(
        count(admin, "SELECT count(*) FROM control.browser_sessions").await,
        1
    );
    assert_eq!(
        count(
            admin,
            "SELECT count(*) FROM control.browser_sessions WHERE revoked_at_us IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            admin,
            "SELECT count(*) FROM control.local_accounts WHERE active"
        )
        .await,
        1
    );
    server.stop();
    assert_eq!(count(admin,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'").await,0);
    case(
        "actual-private-panel-browser",
        json!({"browser":options.browser,"base":options.base(),"cases":8,"sessions":1,"revoked":1,"remaining_control_backends":0}),
    );
}

async fn bucket_state(admin: &Client) -> String {
    admin.query_one("SELECT json_agg(b ORDER BY kind,bucket)::text FROM (SELECT b.*,xmin::text AS xmin FROM control.login_buckets b) b", &[]).await.unwrap().get(0)
}
