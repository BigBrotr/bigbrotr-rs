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
         * Accept a private signed event export job
         * @description Exact Origin, current session, explicit CSRF and Idempotency-Key. Body max8192bytes. New work requires dataset data.read and all three exact-scope actions history.read, export and private-export, expected config/policy revisions, retained native allocation and configured initialized destination. Deployment selection requires migration0813 and job/pin/source-lock privileges before binding. A schema without813 or insufficient job privileges disables this route; existing812 session/history operation remains available. On the available protocol, an identical historical retry needs a live actor session but no current destination, native read or action grants; it never executes work or refreshes pins. A lost reply requires the identical original key/body. Initial transport is under qualification, not a completed exporter. No publication side effect.
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
         * Inspect your export job
         * @description Live browser session, explicit CSRF, current dataset status.read and authenticated actor ownership. GET only, canonical raw ID, no body or query. Requires available0813 job protocol; no archive/native/destination access. Input grants may have been revoked: this exposes only the actor's redacted operational receipt. Missing and other actors' jobs both404. No idle-session touch, background renewal or execution authority. Response socket uses a nonrenewable action window started before SQL. At most one bounded row.
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
        /** @description One actor-owned durable export observation. State is not worker liveness; no input, output, filesystem, execution or download authority. Completion time is present exactly for terminal states; paused/cancel_requested/partial/failed/cancelled/skipped require a redacted reason. Listing and artifact discovery remain separate consumers. */
        JobObservation: {
            /** @constant */
            format: "bigbrotr-job-status/1";
            job_id: string;
            /** @constant */
            kind: "export";
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
            items: components["schemas"]["JobObservation"][];
            next_cursor: components["schemas"]["JobCursor"] | null;
            effective_page_size: number;
            /** @description Canonical signed int64 Unix microseconds. Never round through JavaScript Number. */
            observed_at_us: string;
            /** @constant */
            authority: "observation-only";
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
                "application/json": components["schemas"]["ExportJobCommand"];
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
}
