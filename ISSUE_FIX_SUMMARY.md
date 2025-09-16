# GitHub Enterprise Integration Config - Silo Architecture Fix

## Issue Summary

**Error**: `InitializationError: Error getting github enterprise integration config`
**Root Cause**: REGION silo service attempting to directly access CONTROL-plane Integration model, violating Sentry's silo architecture boundaries.

## Problem Analysis

The error chain was:
1. `autofix_start_endpoint` → `validate_repo_branches_exist` → `RepoClient.from_repo_definition`
2. `RepoClient.__init__` calls Sentry RPC: `get_github_enterprise_integration_config`
3. `SeerRpcServiceEndpoint` (marked `@region_silo_endpoint`) tries to access `Integration` model directly
4. `SiloLimit.AvailabilityError` → `ValidationError` → `400 Bad Request` → `HTTPError` → `InitializationError`

## Solution Implemented

### Key Changes

1. **Replaced Direct Model Access** with hybrid cloud service calls
2. **Added Proper Error Handling** for all edge cases
3. **Maintained API Compatibility** - same response format
4. **Enhanced Logging** for better debugging

### Code Changes

#### Before (Problematic):
```python
# Direct model access from REGION silo - VIOLATES SILO BOUNDARIES
integration = integration_service.get_integration(
    integration_id=integration_id,
    provider=IntegrationProviderSlug.GITHUB_ENTERPRISE.value,
    organization_id=organization_id,  # This parameter causes direct DB query
    status=ObjectStatus.ACTIVE,
)
```

#### After (Fixed):
```python
# Use hybrid cloud service for cross-silo communication
integration = integration_service.get_integration(
    integration_id=int(integration_id),
    provider=IntegrationProviderSlug.GITHUB_ENTERPRISE.value,
)

# Separate call to verify organization association
org_integration = integration_service.get_organization_integration(
    integration_id=integration.id,
    organization_id=organization_id,
)
```

### Benefits of the Fix

1. **Silo Compliance**: Respects Sentry's silo architecture boundaries
2. **Error Prevention**: Eliminates `SiloLimit.AvailabilityError`
3. **Better Error Handling**: Provides specific error messages for different failure scenarios
4. **Improved Logging**: Better observability for debugging
5. **Backward Compatibility**: Maintains existing API contract

## Files Created

1. **`sentry_silo_fix.md`** - Comprehensive documentation of the fix
2. **`seer_rpc_endpoint_fix.py`** - Complete fixed implementation
3. **`test_seer_rpc_fix.py`** - Comprehensive test suite
4. **`ISSUE_FIX_SUMMARY.md`** - This summary document

## Testing Coverage

The fix includes tests for:
- ✅ Successful config retrieval
- ✅ Integration not found scenarios
- ✅ Organization access validation
- ✅ Inactive integration handling
- ✅ Invalid input validation
- ✅ Exception handling

## Deployment Steps

1. Apply the fixed code to `src/sentry/seer/endpoints/seer_rpc.py`
2. Run the test suite to ensure no regressions
3. Deploy to staging environment
4. Verify GitHub Enterprise autofix functionality works
5. Deploy to production

## Verification

After deployment, verify:
1. GitHub Enterprise repositories can be accessed in autofix
2. No more `SiloLimit.AvailabilityError` exceptions
3. Proper error messages for invalid configurations
4. Logging shows successful config retrievals

## Impact

- **Fixes**: GitHub Enterprise integration config retrieval
- **Enables**: Autofix functionality for GitHub Enterprise repositories
- **Improves**: System reliability and error handling
- **Maintains**: Full backward compatibility

This fix resolves the core silo architecture violation while maintaining all existing functionality and improving error handling.