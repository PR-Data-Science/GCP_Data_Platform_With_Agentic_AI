# Apigee Edge Policy Scaffolding

This folder contains starter artifacts for exposing the Phase 2A read-only Agent Service behind Apigee.

## Included artifacts
- `openapi/agent-service-v1.yaml`: versioned API surface (`/health`, `/sessions`, `/router`).
- `policies/VerifyJWT.xml`: token validation policy.
- `policies/Quota-PerClient.xml`: per-client quota policy.
- `policies/AssignMessage-RouteV1.xml`: route/version read-only headers.

## Policy intent
- Enforce authenticated access.
- Apply client-level rate limiting.
- Stamp version/read-only headers for downstream service policy checks.

## Notes
- These are templates and must be wired into your Apigee proxy flow and environment-specific target endpoints.
- Keep write-capable routes out of the proxy until post-Phase 2A controls are approved.
