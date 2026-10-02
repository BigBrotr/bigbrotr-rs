// Generated from contracts/management/v1/openapi.yaml. Do not edit.
export interface paths {
    "/management/v1/sessions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Sign in locally
         * @description Exact same-origin Origin required. Session and CSRF secrets appear only in separate Set-Cookie headers. Unknown completion may have committed; an explicit new login is a new attempt, never an automatic retry. Body max8192bytes; password max1024 UTF-8 bytes.
         */
        post: operations["createSession"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/management/v1/me": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Inspect this session
         * @description Touches idle expiry for actual user activity. Do not poll in the background to keep an unattended session alive. Missing Origin accepted for GET; any supplied Origin must match exactly. Observation alone grants no command or data authority.
         */
        get: operations["inspectSession"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/management/v1/sessions/current": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Sign out
         * @description Requires exact Origin and X-CSRF-Token bound to this cookie. Confirmed204 clears both cookies; an unknown outcome sends no clearing response. Retrying with the same original pair confirms its revocation. No global data-stream drain is implied.
         */
        delete: operations["deleteSession"];
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/management/v1/sessions/current/reauthenticate": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Confirm a sensitive command with the current password
         * @description Same session, exact Origin and explicit CSRF required; current principals.manage action. At most32 retained proofs per account, five minutes or remaining session validity. Password attempts share login quotas and its single owned hash worker. This proof alone does not grant history access; scope-grant acceptance consumes it atomically; native enablement and private history remain under implementation and qualification.
         */
        post: operations["reauthenticateSession"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/management/v1/principals/{principal_id}/grants": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        /**
         * Accept one scoped data grant change
         * @description Exact Origin, current session, CSRF and Idempotency-Key required. Fresh acceptance needs the original session bound to the proof; a historical receipt retry permits a new live session of the same actor. Path principal must equal command target. Body max8192bytes. Requires explicit migration0812 and application SQL privileges. Revocation and historical retries do not depend on Archive. Fresh enablement requires explicit archive.reader.enabled, local Control/Archive custody, separate reader credentials and the installed worker bundle. A known native prerequisite refusal preserves the proof and keeps Control available. Native HTTP execution remains under qualification. This is not complete grant/private-history qualification. A lost response is resolved by an identical command retry; never substitute a new key. Acceptance does not establish applied restrictive drain.
         */
        put: operations["changeScopeGrant"];
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/management/v1/jobs": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List current actor-owned export metadata
         * @description Exact Origin, current session, explicit CSRF and dataset status.read. No input/output grant or idle refresh. Optional protocol requires0816, loaded registered material and an active unrevoked issuer, including empty/final pages. Query is closed; no duplicate/empty/percent-encoded values, body or HEAD. Initial listing captures an immutable admission upper bound. Current state filters are reevaluated per page. Current policy/grant bindings, original absolute lifetime and key revocation apply to every continuation. Retention commits before token delivery; no token on unknown COMMIT. HTTP/native qualification is scoped; full fault/browser acceptance remains open.
         */
        get: operations["listJobs"];
        put?: never;
        /**
         * Accept selected finite work
         * @description Closed8KiB typed finite work. Operational Serve accepts export; the explicit initial Ingest owner accepts acquisition. Other kinds/profile combinations refuse400. Session/CSRF/Origin and Idempotency-Key are required. Fresh202 and exact historical200 return acceptance only. Acquisition requires roles.control; scope/duration/expected heads remain immutable across retries. No automatic execution or native effect is implied.
         */
        post: operations["acceptJob"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/management/v1/jobs/{job_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Inspect your selected job
         * @description Actor-owned protocol1 operational status: export on ordinary Serve, acquisition on the explicit initial owner. Current session, CSRF and status.read required. Foreign/missing/unsupported job IDs all return404. No activity refresh, scope, selector, duration, source, artifact or execution authority. The original authenticated delivery deadline constrains actual socket writes.
         */
        get: operations["observeJob"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/management/v1/jobs/{job_id}/resume": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Resume your paused signed export
         * @description Exact Origin, live actor session, explicit CSRF and original Idempotency-Key; canonical raw job ID, no query, body max8192bytes. New resume requires expected job/config/policy revisions, current data.read plus exact-scope history.read/export/private-export and the original configured destination. No roles.control, status.read or reauthentication proof. It queues the same committed unit and retains the original pin/caps; no fresh reader is needed for acceptance. Actual execution independently obtains current reader authority and output exclusion. Identical historical retry resolves before current grants/destination/job state and cannot restart a second pause. No idle-session touch or cookies. Schema0814 and explicit resume privileges required; an unavailable optional protocol returns503 without disabling earlier session routes. Under installed HTTP qualification.
         */
        post: operations["resumeExport"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/management/v1/relay-enrollments": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Accept a direct relay enrollment change
         * @description Available on the explicit initial Ingest owner only, with administration enabled; the ordinary Serve profile does not expose this route. Exact Origin, live session, explicit CSRF and one canonical Idempotency-Key required; no query or HEAD. Fresh commands require dataset policy.change and exact expected config/policy revisions. Public enablement intersects both complete accepted policy documents; private enablement must fit a complete deployment root. Migration0026 (including enrollment prerequisites) and all required table and function privileges must be available before the initial listener binds. Historical retry authenticates the current actor then resolves before fresh permission, CAS and root checks. A lost response requires the identical original key and body. This command consumes an existing EndpointId; relay-endpoints supplies authenticated URL registration. No installed acquisition or restrictive drain claim. Native installed qualification is scoped to the referenced initial HTTP journey.
         */
        post: operations["changeRelayEnrollment"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/management/v1/relay-endpoints": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Accept an immutable relay endpoint identity
         * @description Available on the explicit initial Ingest owner only, with administration enabled; the ordinary Serve profile does not expose this route. Exact Origin, live session, explicit CSRF and one canonical Idempotency-Key required; no query or HEAD. Fresh commands require dataset policy.change and exact expected config/policy revisions. Migration0026 and required table/function privileges must be available before binding. Equivalent normalized URLs share an EndpointId, including under a different command key. Historical retry authenticates the current actor and resolves the original normalized command before fresh permission and CAS checks. After a lost response use the identical original key and body; never retry mutations automatically. No cookies, idle refresh, enrollment, DNS, socket or native dictionary work. Native installed qualification covers the bounded HTTP journey; COMMIT fault qualification remains separate.
         */
        post: operations["registerRelayEndpoint"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/management/v1/acquisition-scopes": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Accept an immutable acquisition scope identity
         * @description Available on the explicit initial Ingest owner with administration enabled. Migration0026 and selected table/function privileges are required before bind. Exact Origin, current session, explicit CSRF and one canonical Idempotency-Key; no query or HEAD. Fresh acceptance requires dataset roles.control and exact expected config/policy revisions. Scope epoch equals the original expected policy revision. Missing or mismatched enrollment/access/visibility is a conflict, without stopping the owner. Authenticate historical actor/key retry before fresh permission and CAS; normalized equivalent selectors preserve the same receipt. Preserve the exact original command after response loss; no automatic mutation retry. No cookies, idle refresh, enrollment changes, network dispatch, native writes, visibility grants or coverage. Implementation and native qualification are in progress.
         */
        post: operations["registerAcquisitionScope"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
}
export type webhooks = Record<string, never>;
export interface components {
    schemas: {
        LoginInput: {
            username: string;
            /** @description Exact password, at most1024 UTF-8 bytes and no control characters. Never trim or normalize. */
            password: string;
        };
        SessionObservation: {
            /** @constant */
            format: "bigbrotr-session/1";
            principal_id: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            credential_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            grant_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. Configuration head checked in the same transaction as the policy/session observation; input for expected revisions, never command authority. */
            config_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            policy_revision: string;
            /** @description Positive Unix microseconds, maximum9223372036854775807. A server observation, not a renewable client-side authority lease. */
            expires_at_us: string;
            /** @description Positive Unix microseconds, maximum9223372036854775807. A server observation, not a renewable client-side authority lease. */
            idle_expires_at_us: string;
            stored_actions: ("audit.read" | "budgets.change" | "data.read" | "policy.change" | "principals.manage" | "roles.control" | "status.read")[];
            /** @constant */
            authority: "observation-only";
        };
        ApiError: {
            error: {
                /** @enum {integer} */
                status: 400 | 401 | 403 | 413 | 415 | 429 | 503 | 409 | 404 | 410;
                /** @enum {string} */
                code: "invalid_request" | "authentication_denied" | "browser_boundary_denied" | "request_too_large" | "unsupported_media_type" | "capacity_or_rate_limited" | "authority_unavailable" | "permission_denied" | "command_conflict" | "job_unavailable" | "invalid_cursor" | "cursor_incompatible" | "cursor_expired";
                /** @enum {string} */
                message: "Invalid request." | "Authentication required or denied." | "Browser request boundary refused." | "Request exceeds its byte limit." | "JSON content type required." | "Request capacity or attempt limit reached." | "Current authority or dependency unavailable." | "This action is not permitted." | "Command key or expected revision conflicts with current state." | "Job unavailable to this account." | "Continuation token refused." | "Continuation no longer matches this request or authority." | "Continuation expired. Start a new listing.";
                /** @enum {string} */
                retry_class: "authenticate" | "check_state" | "correct_request" | "retry_later";
                correlation_id: string | null;
            } & ({
                /** @constant */
                status?: 400;
                /** @constant */
                code?: "invalid_request";
                /** @constant */
                message?: "Invalid request.";
                /** @constant */
                retry_class?: "correct_request";
            } | {
                /** @constant */
                status?: 401;
                /** @constant */
                code?: "authentication_denied";
                /** @constant */
                message?: "Authentication required or denied.";
                /** @constant */
                retry_class?: "authenticate";
            } | {
                /** @constant */
                status?: 403;
                /** @constant */
                code?: "browser_boundary_denied";
                /** @constant */
                message?: "Browser request boundary refused.";
                /** @constant */
                retry_class?: "correct_request";
            } | {
                /** @constant */
                status?: 413;
                /** @constant */
                code?: "request_too_large";
                /** @constant */
                message?: "Request exceeds its byte limit.";
                /** @constant */
                retry_class?: "correct_request";
            } | {
                /** @constant */
                status?: 415;
                /** @constant */
                code?: "unsupported_media_type";
                /** @constant */
                message?: "JSON content type required.";
                /** @constant */
                retry_class?: "correct_request";
            } | {
                /** @constant */
                status?: 429;
                /** @constant */
                code?: "capacity_or_rate_limited";
                /** @constant */
                message?: "Request capacity or attempt limit reached.";
                /** @constant */
                retry_class?: "retry_later";
            } | {
                /** @constant */
                status?: 503;
                /** @constant */
                code?: "authority_unavailable";
                /** @constant */
                message?: "Current authority or dependency unavailable.";
                /** @constant */
                retry_class?: "check_state";
            } | {
                /** @constant */
                status?: 403;
                /** @constant */
                code?: "permission_denied";
                /** @constant */
                message?: "This action is not permitted.";
                /** @constant */
                retry_class?: "check_state";
            } | {
                /** @constant */
                status?: 409;
                /** @constant */
                code?: "command_conflict";
                /** @constant */
                message?: "Command key or expected revision conflicts with current state.";
                /** @constant */
                retry_class?: "check_state";
            } | {
                /** @constant */
                status?: 404;
                /** @constant */
                code?: "job_unavailable";
                /** @constant */
                message?: "Job unavailable to this account.";
                /** @constant */
                retry_class?: "check_state";
            } | {
                /** @constant */
                status?: 400;
                /** @constant */
                code?: "invalid_cursor";
                /** @constant */
                message?: "Continuation token refused.";
                /** @constant */
                retry_class?: "correct_request";
            } | {
                /** @constant */
                status?: 409;
                /** @constant */
                code?: "cursor_incompatible";
                /** @constant */
                message?: "Continuation no longer matches this request or authority.";
                /** @constant */
                retry_class?: "check_state";
            } | {
                /** @constant */
                status?: 410;
                /** @constant */
                code?: "cursor_expired";
                /** @constant */
                message?: "Continuation expired. Start a new listing.";
                /** @constant */
                retry_class?: "check_state";
            });
        };
        /** @description Exact desired command, not authority. Scope dataset must equal the installed dataset. Enabling requires allocation; disabling requires null. Revisions/ordinal must fit positive uint64. */
        ScopeGrantCommand: {
            principal_id: string;
            /** @enum {string} */
            action: "history.read" | "export" | "private-export";
            scope: {
                dataset_id: string;
                /** @constant */
                dictionary_kind: 4;
                allocation_domain: string;
                /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
                ordinal: string;
            };
            enabled: boolean;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_config_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_policy_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_grant_revision: string;
            allocation: {
                catalog_id: string;
                /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
                frontier_revision: string;
            } | null;
        };
        ReauthenticationInput: {
            /** @description Exact password, at most1024 UTF-8 bytes and no control characters. Never trim or normalize. */
            password: string;
            command: components["schemas"]["ScopeGrantCommand"];
        };
        ReauthenticationProof: {
            /** @constant */
            format: "bigbrotr-reauthentication/1";
            proof_id: string;
            command_key: string;
            command_digest: string;
            /** @description Positive Unix microseconds, maximum9223372036854775807. A server observation, not a renewable client-side authority lease. */
            expires_at_us: string;
            /** @constant */
            authority: "same-session-command-only";
        };
        ScopeGrantInput: {
            proof_id: string;
            command: components["schemas"]["ScopeGrantCommand"];
        };
        /** @description Immutable acceptance observation. An identical historical retry returns created=false and the original result; it never reapplies state or proves restrictive drain. */
        ScopeGrantReceipt: {
            /** @constant */
            format: "bigbrotr-scope-grant/1";
            command_key: string;
            principal_id: string;
            scope: {
                dataset_id: string;
                /** @constant */
                dictionary_kind: 4;
                allocation_domain: string;
                /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
                ordinal: string;
            };
            enabled: boolean;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            scope_generation: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            account_grant_revision: string;
            /** @description Original positive Unix microseconds, maximum9223372036854775807; exact decimal string. */
            accepted_at_us: string;
            created: boolean;
            /** @constant */
            application: "unobserved";
            /** @constant */
            authority: "acceptance-only";
        };
        /** @description Initial private exact-ID signed JSONL job profile. Query defaults use immutable protocol defaults: snapshot and page_size100. Complete scope and approved destination binding required; no path or actor supplied by the browser. Native acceptance and automatic signed JSONL execution are under qualification; the acceptance receipt itself does not observe execution. */
        ExportJobCommand: {
            /** @constant */
            kind: "export";
            /** @constant */
            format: "signed-events-jsonl";
            query: components["schemas"]["request.schema"];
            allocation: {
                catalog_id: string;
                /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
                frontier_revision: string;
            };
            destination: {
                id: string;
                binding_sha256: string;
            };
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_config_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_policy_revision: string;
        };
        /** @description Immutable historical acceptance only, not current job state, execution or artifact availability. An exact retry preserves job identity/time and does not refresh or reapply effects. */
        JobAcceptance: {
            /** @constant */
            format: "bigbrotr-job-acceptance/1";
            command_key: string;
            job_id: string;
            /** @description Original positive Unix microseconds, maximum9223372036854775807; exact decimal string. */
            accepted_at_us: string;
            created: boolean;
            /** @constant */
            execution: "unobserved";
            /** @constant */
            authority: "acceptance-only";
        };
        /** @description One actor-owned durable job observation (export on operational Serve, acquisition on the initial owner). State is not worker liveness; no input, output, filesystem, execution or download authority. Completion time is present exactly for terminal states; paused/cancel_requested/partial/failed/cancelled/skipped require a redacted reason. Listing and artifact discovery remain separate consumers. */
        JobObservation: {
            /** @constant */
            format: "bigbrotr-job-status/1";
            job_id: string;
            /** @enum {string} */
            kind: "export" | "acquisition";
            /** @enum {string} */
            state: "accepted" | "queued" | "running" | "paused" | "cancel_requested" | "succeeded" | "partial" | "failed" | "cancelled" | "skipped";
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            job_revision: string;
            /** @description Canonical uint64 decimal string, maximum18446744073709551615. */
            checkpoint_revision: string;
            /** @description Canonical signed int64 Unix microseconds. Never round through JavaScript Number. */
            created_at_us: string;
            completed_at_us: string | null;
            /** @description Canonical signed int64 Unix microseconds. Never round through JavaScript Number. */
            observed_at_us: string;
            /** @enum {string|null} */
            reason_code: null | "resource_limit" | "execution_refused" | "other";
            /** @constant */
            authority: "observation-only";
        };
        /** @description Resume the same actor-owned paused signed-export unit. No selection, destination or cap change. A new resume key differs from the original export key; identical retry uses its original key and body. */
        ResumeExportCommand: {
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_job_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_config_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_policy_revision: string;
        };
        /** @description Immutable historical resume acceptance. job_revision is the originally accepted revision, even after another pause. No current state, execution, output or download authority. */
        ResumeExportAcceptance: {
            /** @constant */
            format: "bigbrotr-export-resume/1";
            command_key: string;
            job_id: string;
            /** @description Original positive Unix microseconds, maximum9223372036854775807; exact decimal string. */
            accepted_at_us: string;
            created: boolean;
            /** @constant */
            execution: "unobserved";
            /** @constant */
            authority: "acceptance-only";
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            job_revision: string;
        };
        /** @description Opaque continuation, never a substitute for current session/permission. Repeat the same normalized filters. Do not decode, persist or log it. */
        JobCursor: string;
        JobListQuery: {
            /** @constant */
            kind?: "export";
            /** @enum {string} */
            state?: "accepted" | "queued" | "running" | "paused" | "cancel_requested" | "succeeded" | "partial" | "failed" | "cancelled" | "skipped";
            /** @description Inclusive canonical signed int64 Unix microseconds. */
            created_from_us?: string;
            /** @description Exclusive canonical signed int64 Unix microseconds, strictly greater than created_from_us when both are present. */
            created_before_us?: string;
            /** @description Default50, clamped to200. Continuation binds the effective value. */
            page_size?: number;
            cursor?: components["schemas"]["JobCursor"];
        };
        /** @description At most effective_page_size current actor-owned export observations, descending(created_at_us,job_id). Empty pages may advance: next_cursor null alone means completion. Candidate scan is bounded before current mutable filters/admission-cut checks. No scientific snapshot, frozen membership, input/output or execution authority. JSON response max128KiB; no automatic fetch-all. */
        JobPage: {
            /** @constant */
            format: "bigbrotr-job-list/1";
            items: (components["schemas"]["JobObservation"] & {
                /** @constant */
                kind?: "export";
            })[];
            next_cursor: components["schemas"]["JobCursor"] | null;
            effective_page_size: number;
            /** @description Canonical signed int64 Unix microseconds. Never round through JavaScript Number. */
            observed_at_us: string;
            /** @constant */
            authority: "observation-only";
        };
        RelayEnrollmentRoute: {
            /** @constant */
            class: "public";
        } | {
            /** @constant */
            class: "private";
            /** @description IPv4 or IPv6 network/prefix with clear host bits; mapped IPv6 refused. Equivalent IPv6 spellings normalize to identical command bytes. The server parses and validates the complete prefix. */
            cidr: string;
        };
        /** @description Declared source intent for an already registered endpoint in the installed dataset. First enablement requires explicit null expected_generation; existing transitions require the exact current generation. Withdrawal must retain the route. Visibility must name this dataset and kind4; this command does not prove the native mapping or grant network/data authority. Unknown fields and duplicate JSON keys are refused by the server. The complete UTF-8 request body is limited to8192bytes. */
        RelayEnrollmentCommand: {
            endpoint_id: string;
            visibility: components["schemas"]["ScopeGrantCommand"]["scope"];
            route: components["schemas"]["RelayEnrollmentRoute"];
            enabled: boolean;
            expected_generation: string | null;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_config_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_policy_revision: string;
        } & unknown;
        /** @description Immutable original acceptance. Historical retry returns created=false and may describe an earlier enabled generation after withdrawal; it never reapplies that state or establishes restrictive drain. */
        RelayEnrollmentReceipt: {
            /** @constant */
            format: "bigbrotr-relay-enrollment/1";
            command_key: string;
            endpoint_id: string;
            access_scope_id: string;
            route_id: string;
            enabled: boolean;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            generation: string;
            /** @description Original positive Unix microseconds, maximum9223372036854775807; exact decimal string. */
            accepted_at_us: string;
            created: boolean;
            /** @constant */
            application: "unobserved";
            /** @constant */
            authority: "acceptance-only";
        };
        /** @description Identity-only intent in the installed dataset. Unknown fields and duplicate JSON keys are refused. The complete UTF-8 JSON request body is limited to17408bytes (17KiB), allowing an8192-byte endpoint with escaped quotes. Equivalent normalized URLs produce the same command digest; raw JSON spelling is not command identity. */
        RelayRegistrationCommand: {
            /** @description Input to the single endpoint-v1 normalizer: an absolute ws/wss URL, including uppercase scheme spelling; no credentials, fragment or forbidden host. Both input and normalized identity are limited to8192 UTF-8 bytes by the server. maxLength is only a character bound, not the byte or semantic validator. Normalization preserves query bytes/order and explicitly empty query. */
            url: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_config_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_policy_revision: string;
        };
        /** @description Immutable original command acceptance; created=true for a fresh command, including one naming an existing endpoint. Historical retry returns created=false with the original identity and time. This receipt grants no enrollment, route, visibility or network authority. */
        RelayRegistrationReceipt: {
            /** @constant */
            format: "bigbrotr-relay-registration/1";
            command_key: string;
            endpoint_id: string;
            /** @description Original positive Unix microseconds, maximum9223372036854775807; exact decimal string. */
            accepted_at_us: string;
            created: boolean;
            /** @constant */
            authority: "identity-only";
        };
        /** @description Profile1 conjunction. Full lowercase64-hex IDs/authors; tags have exactly one ASCII letter. Empty object selects all structurally matching events. Duplicate keys and unsupported fields (including limit/search) refuse. Optional null list/time fields mean absent. Value lists are sorted and deduplicated. Actual profile also bounds total values/text, not only individual fields. */
        AcquisitionFilter: {
            ids?: string[] | null;
            authors?: string[] | null;
            kinds?: number[] | null;
            /** @description Exact Nostr uint64 seconds as a JSON integer. JavaScript Number cannot represent all accepted values; use lossless serialization for values above9007199254740991. */
            since?: number | null;
            /** @description Exact Nostr uint64 seconds as a JSON integer. JavaScript Number cannot represent all accepted values; use lossless serialization for values above9007199254740991. */
            until?: number | null;
            "#e"?: string[];
            "#p"?: string[];
        } & {
            [key: string]: string[];
        };
        /** @description Existing profile1 collection intent, with no acquisition authority. Server normalizes filter unions/lists, compares complete canonical bytes and enforces16384 total values,1MiB UTF-8 tag text,2MiB selector input/canonical document and finite parser work. Character/count schema bounds do not replace these byte or semantic checks. */
        CollectionSelector: {
            /** @constant */
            version: 1;
            filters: components["schemas"]["AcquisitionFilter"][];
            /** @description Exact Nostr uint64 seconds as a JSON integer. JavaScript Number cannot represent all accepted values; use lossless serialization for values above9007199254740991. */
            history_since: number;
            /** @enum {string} */
            ephemeral: "retain" | "drop";
        };
        /** @description Closed identity-only command. Complete UTF-8 body at most2098176bytes; selector at most2097152bytes. Duplicate keys refuse.14MiB+64KiB request reservation stays with queued/SQL work after HTTP disconnect; capacity refusal is429. Scope epoch is the exact expected policy revision. */
        AcquisitionScopeCommand: {
            endpoint_id: string;
            access_scope_id: string;
            visibility: components["schemas"]["ScopeGrantCommand"]["scope"];
            selector: components["schemas"]["CollectionSelector"];
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_config_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_policy_revision: string;
        };
        /** @description Original authenticated command acceptance, preserving immutable scope attribution. created reflects this command, including an existing scope; retry returns original identity/time with created=false. No execution, read or visibility authority. */
        AcquisitionScopeReceipt: {
            /** @constant */
            format: "bigbrotr-acquisition-scope-registration/1";
            command_key: string;
            /** @description Original positive Unix microseconds, maximum9223372036854775807; exact decimal string. */
            accepted_at_us: string;
            created: boolean;
            /** @constant */
            authority: "identity-only";
            scope_id: string;
        };
        /** @description Initial-owner finite configured Sync intent over one registered scope. Requires current session/CSRF, roles.control and original expected heads. Acceptance grants no socket, data access or coverage; installed execution remains in progress. */
        AcquisitionJobCommand: {
            /** @constant */
            kind: "acquisition";
            scope_id: string;
            /** @description Original duration in microseconds, from1 through86400000000 (24h). Starts at acceptance, including COMMIT waiting; retries never extend it. */
            deadline_us: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_config_revision: string;
            /** @description Canonical positive uint64 decimal string; maximum18446744073709551615. Never convert to JavaScript Number. */
            expected_policy_revision: string;
        };
        /**
         * Initial exact-ID history QueryRequest
         * @description Private integration profile. Broader selected resources, filters and cursors remain required work; reject rather than ignore unsupported constraints. Duplicate JSON keys are invalid; schemas alone cannot detect them.
         */
        "request.schema": {
            /** @constant */
            version: 1;
            /** @constant */
            resource: "event_history";
            /**
             * @default snapshot
             * @constant
             */
            mode: "snapshot";
            scope: {
                dataset_id: string;
                /** @constant */
                dictionary_kind: 4;
                allocation_domain: string;
                /** @description Canonical unsigned decimal; runtime also enforces uint64 maximum18446744073709551615. */
                ordinal: string;
            };
            filters: {
                ids: string[];
            }[];
            /** @default 100 */
            page_size: number | null;
            cursor?: null;
            projection?: null;
        };
    };
    responses: never;
    parameters: never;
    requestBodies: never;
    headers: never;
    pathItems: never;
}
export type $defs = Record<string, never>;
export interface operations {
    createSession: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["LoginInput"];
            };
        };
        responses: {
            /** @description New session; HttpOnly session cookie and readable CSRF cookie */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SessionObservation"];
                };
            };
            /** @description Invalid request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Body byte limit */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description JSON required */
            415: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
    inspectSession: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Current session observation */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SessionObservation"];
                };
            };
            /** @description Invalid request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Body byte limit */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description JSON required */
            415: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
    deleteSession: {
        parameters: {
            query?: never;
            header: {
                "X-CSRF-Token": string;
            };
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Confirmed revocation; two cookie clearing headers and no body */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Invalid request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Body byte limit */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description JSON required */
            415: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
    reauthenticateSession: {
        parameters: {
            query?: never;
            header: {
                "X-CSRF-Token": string;
                "Idempotency-Key": string;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ReauthenticationInput"];
            };
        };
        responses: {
            /** @description Same-session command proof; no cookies issued. A verified identical retry preserves original expiry and audit. */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ReauthenticationProof"];
                };
            };
            /** @description Invalid request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Command or revision conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Body byte limit */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description JSON required */
            415: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
    changeScopeGrant: {
        parameters: {
            query?: never;
            header: {
                "X-CSRF-Token": string;
                "Idempotency-Key": string;
            };
            path: {
                principal_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ScopeGrantInput"];
            };
        };
        responses: {
            /** @description Accepted command or identical historical receipt; no cookies and no read capability. */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ScopeGrantReceipt"];
                };
            };
            /** @description Invalid request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Command or revision conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Body byte limit */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description JSON required */
            415: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
    listJobs: {
        parameters: {
            query?: {
                kind?: components["schemas"]["JobListQuery"]["kind"];
                state?: components["schemas"]["JobListQuery"]["state"];
                created_from_us?: components["schemas"]["JobListQuery"]["created_from_us"];
                created_before_us?: components["schemas"]["JobListQuery"]["created_before_us"];
                page_size?: components["schemas"]["JobListQuery"]["page_size"];
                cursor?: components["schemas"]["JobListQuery"]["cursor"];
            };
            header: {
                "X-CSRF-Token": string;
            };
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Bounded current observations, possibly empty with a continuation */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobPage"];
                };
            };
            /** @description Invalid query or refused continuation */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Continuation no longer matches this request or authority */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Continuation expired; explicitly start a new listing */
            410: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
    acceptJob: {
        parameters: {
            query?: never;
            header: {
                "X-CSRF-Token": string;
                "Idempotency-Key": string;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ExportJobCommand"] | components["schemas"]["AcquisitionJobCommand"];
            };
        };
        responses: {
            /** @description Identical historical acceptance for this actor/key; no execution claim. */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobAcceptance"];
                };
            };
            /** @description New atomic command, job and retained input pin accepted. */
            202: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobAcceptance"];
                };
            };
            /** @description Invalid request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Command or revision conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Body byte limit */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description JSON required */
            415: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
    observeJob: {
        parameters: {
            query?: never;
            header: {
                "X-CSRF-Token": string;
            };
            path: {
                job_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Current durable metadata; no cookies or idle refresh. */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobObservation"];
                };
            };
            /** @description Invalid request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Missing or invisible export; same response. */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
    resumeExport: {
        parameters: {
            query?: never;
            header: {
                "X-CSRF-Token": string;
                "Idempotency-Key": string;
            };
            path: {
                job_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ResumeExportCommand"];
            };
        };
        responses: {
            /** @description Identical historical acceptance for this actor/key; no execution claim. */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ResumeExportAcceptance"];
                };
            };
            /** @description New atomic resume receipt and same-unit queued revision accepted. */
            202: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ResumeExportAcceptance"];
                };
            };
            /** @description Invalid request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Command or revision conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Body byte limit */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description JSON required */
            415: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
    changeRelayEnrollment: {
        parameters: {
            query?: never;
            header: {
                "X-CSRF-Token": string;
                "Idempotency-Key": string;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["RelayEnrollmentCommand"];
            };
        };
        responses: {
            /** @description Identical authenticated actor/key/body returns only the original receipt; no cookies or idle refresh. */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["RelayEnrollmentReceipt"];
                };
            };
            /** @description Fresh enrollment transition accepted atomically; no cookies or network work. */
            202: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["RelayEnrollmentReceipt"];
                };
            };
            /** @description Invalid request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary, dataset permission or current route policy denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Command or revision conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Body byte limit */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description JSON required */
            415: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
    registerRelayEndpoint: {
        parameters: {
            query?: never;
            header: {
                "X-CSRF-Token": string;
                "Idempotency-Key": string;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["RelayRegistrationCommand"];
            };
        };
        responses: {
            /** @description Identical authenticated actor/key/body returns only the original receipt; no cookies or idle refresh. */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["RelayRegistrationReceipt"];
                };
            };
            /** @description Fresh command accepted atomically, even when the endpoint already exists; created describes the command receipt. */
            202: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["RelayRegistrationReceipt"];
                };
            };
            /** @description Invalid request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary or dataset permission denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Command or revision conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Body byte limit */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description JSON required */
            415: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
    registerAcquisitionScope: {
        parameters: {
            query?: never;
            header: {
                "X-CSRF-Token": string;
                "Idempotency-Key": string;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AcquisitionScopeCommand"];
            };
        };
        responses: {
            /** @description Identical authenticated actor/key/body returns only the original receipt; no cookies or idle refresh. */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AcquisitionScopeReceipt"];
                };
            };
            /** @description Fresh command accepted atomically, including an already registered scope; created describes command acceptance. */
            202: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AcquisitionScopeReceipt"];
                };
            };
            /** @description Invalid request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authentication denied */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Browser boundary or dataset permission denied */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Command/revision conflict or missing/mismatched source tuple */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Body byte limit */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description JSON required */
            415: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Capacity or rate limit; Retry-After60 seconds */
            429: {
                headers: {
                    /** @description Initial fixed retry delay, in seconds; clients must not retry mutations automatically. */
                    "Retry-After"?: "60";
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
            /** @description Authority or dependency unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiError"];
                };
            };
        };
    };
}
