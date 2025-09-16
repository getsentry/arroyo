# Sentry Silo Architecture Fix for GitHub Enterprise Integration Config

## Problem
The `get_github_enterprise_integration_config` RPC method in `SeerRpcServiceEndpoint` is violating silo boundaries by attempting to directly access the `Integration` model from a REGION silo service.

## Root Cause
```python
# PROBLEMATIC CODE (in src/sentry/seer/endpoints/seer_rpc.py)
@region_silo_endpoint
class SeerRpcServiceEndpoint(Endpoint):
    def get_github_enterprise_integration_config(self, integration_id: str, organization_id: int):
        # This violates silo boundaries - REGION silo accessing CONTROL model directly
        integration = integration_service.get_integration(
            integration_id=integration_id,
            provider=IntegrationProviderSlug.GITHUB_ENTERPRISE.value,
            organization_id=organization_id,
            status=ObjectStatus.ACTIVE,
        )
```

## Solution

### Option 1: Use RPC Service for Cross-Silo Communication

Replace the direct model access with an RPC call to the CONTROL silo:

```python
# FIXED CODE
from sentry.services.hybrid_cloud.integration import integration_service
from sentry.silo.base import SiloMode
from sentry.types.region import get_local_region

@region_silo_endpoint
class SeerRpcServiceEndpoint(Endpoint):
    def get_github_enterprise_integration_config(self, integration_id: str, organization_id: int):
        try:
            # Use the proper service interface that handles cross-silo communication
            integration = integration_service.get_integration(
                integration_id=integration_id,
                provider=IntegrationProviderSlug.GITHUB_ENTERPRISE.value,
                organization_id=organization_id,
                status=ObjectStatus.ACTIVE,
            )
            
            if not integration:
                return {"success": False, "error": "Integration not found"}
                
            # Extract the needed configuration from the integration
            config = {
                "success": True,
                "base_url": integration.metadata.get("base_url"),
                "api_url": integration.metadata.get("api_url"),
                "installation_id": integration.external_id,
                "app_id": integration.metadata.get("app_id"),
            }
            
            return config
            
        except Exception as e:
            logger.exception("Error retrieving GitHub Enterprise integration config")
            return {"success": False, "error": str(e)}
```

### Option 2: Move to Control Silo Endpoint

If the RPC service doesn't properly handle cross-silo calls, move this endpoint to the CONTROL silo:

```python
# Move from @region_silo_endpoint to @control_silo_endpoint
from sentry.silo.base import control_silo_endpoint

@control_silo_endpoint
class SeerRpcServiceEndpoint(Endpoint):
    def get_github_enterprise_integration_config(self, integration_id: str, organization_id: int):
        # Now we can safely access Integration models directly
        try:
            integration = Integration.objects.get(
                id=integration_id,
                provider=IntegrationProviderSlug.GITHUB_ENTERPRISE.value,
                organizationintegration__organization_id=organization_id,
                status=ObjectStatus.ACTIVE,
            )
            
            config = {
                "success": True,
                "base_url": integration.metadata.get("base_url"),
                "api_url": integration.metadata.get("api_url"),
                "installation_id": integration.external_id,
                "app_id": integration.metadata.get("app_id"),
            }
            
            return config
            
        except Integration.DoesNotExist:
            return {"success": False, "error": "Integration not found"}
        except Exception as e:
            logger.exception("Error retrieving GitHub Enterprise integration config")
            return {"success": False, "error": str(e)}
```

### Option 3: Use Hybrid Cloud Service (Recommended)

Use Sentry's hybrid cloud service pattern for proper cross-silo communication:

```python
from sentry.services.hybrid_cloud.integration import integration_service
from sentry.silo.base import region_silo_endpoint

@region_silo_endpoint
class SeerRpcServiceEndpoint(Endpoint):
    def get_github_enterprise_integration_config(self, integration_id: str, organization_id: int):
        try:
            # Use the hybrid cloud service which handles silo boundaries properly
            integration = integration_service.get_integration(
                integration_id=int(integration_id),
                provider=IntegrationProviderSlug.GITHUB_ENTERPRISE.value,
            )
            
            if not integration:
                return {"success": False, "error": "Integration not found"}
                
            # Verify the integration belongs to the organization
            org_integration = integration_service.get_organization_integration(
                integration_id=integration.id,
                organization_id=organization_id,
            )
            
            if not org_integration:
                return {"success": False, "error": "Integration not found for organization"}
            
            config = {
                "success": True,
                "base_url": integration.metadata.get("base_url"),
                "api_url": integration.metadata.get("api_url"), 
                "installation_id": integration.external_id,
                "app_id": integration.metadata.get("app_id"),
            }
            
            return config
            
        except Exception as e:
            logger.exception("Error retrieving GitHub Enterprise integration config")
            return {"success": False, "error": str(e)}
```

## Files to Modify

1. **src/sentry/seer/endpoints/seer_rpc.py** - Update the `get_github_enterprise_integration_config` method
2. **src/sentry/integrations/services/integration/impl.py** - Ensure the service properly handles cross-silo calls
3. **tests/** - Update relevant tests to account for the new implementation

## Testing

1. Test the RPC call from Seer to ensure it returns proper configuration
2. Verify that GitHub Enterprise repositories can be accessed after the fix
3. Test error handling for non-existent integrations
4. Ensure no other silo violations are introduced

## Migration Notes

- This fix maintains backward compatibility
- No database migrations required
- The API response format remains the same
- Error handling is improved with proper logging