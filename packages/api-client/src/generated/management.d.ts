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
         * @description Same session, exact Origin and explicit CSRF required; current principals.manage action. At most32 retained proofs per account, five minutes or remaining session validity. Password attempts share login quotas and its single owned hash worker. This proof alone does not grant history access; scoped command consumption remains under implementation and qualification.
         */
        post: operations["reauthenticateSession"];
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
                status: 400 | 401 | 403 | 413 | 415 | 429 | 503 | 409;
                /** @enum {string} */
                code: "invalid_request" | "authentication_denied" | "browser_boundary_denied" | "request_too_large" | "unsupported_media_type" | "capacity_or_rate_limited" | "authority_unavailable" | "permission_denied" | "command_conflict";
                /** @enum {string} */
                message: "Invalid request." | "Authentication required or denied." | "Browser request boundary refused." | "Request exceeds its byte limit." | "JSON content type required." | "Request capacity or attempt limit reached." | "Current authority or dependency unavailable." | "This action is not permitted." | "Command key or expected revision conflicts with current state.";
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
            });
        };
        /** @description Exact desired command, not authority. Scope dataset must equal the installed dataset. Enabling requires allocation; disabling requires null. Revisions/ordinal must fit positive uint64. */
        ScopeGrantCommand: {
            principal_id: string;
            /** @constant */
            action: "history.read";
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
}
