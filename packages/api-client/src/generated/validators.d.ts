// Generated from contracts/management/v1/openapi.yaml. Do not edit.
import type { components } from './management';
export declare function validateSession(value: unknown): value is components['schemas']['SessionObservation'];
export declare function validateError(value: unknown): value is components['schemas']['ApiError'];
export declare function validateJob(value: unknown): value is components['schemas']['JobObservation'];
export declare function validateRoles(value: unknown): value is components['schemas']['RoleStatusObservation'];
export declare function validateJobListQuery(value: unknown): value is components['schemas']['JobListQuery'];
export declare function validateJobPage(value: unknown): value is components['schemas']['JobPage'];
export declare function validateResumeCommand(value: unknown): value is components['schemas']['ResumeExportCommand'];
export declare function validateResumeReceipt(value: unknown): value is components['schemas']['ResumeExportAcceptance'];
