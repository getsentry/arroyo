# Fixed implementation for src/sentry/seer/endpoints/seer_rpc.py
# This addresses the silo architecture violation in get_github_enterprise_integration_config

from sentry.api.base import Endpoint
from sentry.api.serializers import serialize
from sentry.services.hybrid_cloud.integration import integration_service
from sentry.silo.base import region_silo_endpoint
from sentry.types.integrations import IntegrationProviderSlug
from sentry.models.integrations.integration import Integration
from sentry.models.organizationintegration import OrganizationIntegration
from sentry.constants import ObjectStatus
import logging

logger = logging.getLogger(__name__)

@region_silo_endpoint
class SeerRpcServiceEndpoint(Endpoint):
    """
    Fixed implementation that properly handles silo boundaries
    """
    
    def get_github_enterprise_integration_config(self, integration_id: str, organization_id: int):
        """
        Retrieve GitHub Enterprise integration configuration using proper silo-safe methods.
        
        This method has been fixed to avoid direct model access from REGION silo,
        which was causing SiloLimit.AvailabilityError.
        """
        try:
            # Convert integration_id to int if it's a string
            integration_id_int = int(integration_id)
            
            # Use the hybrid cloud service which properly handles cross-silo communication
            integration = integration_service.get_integration(
                integration_id=integration_id_int,
                provider=IntegrationProviderSlug.GITHUB_ENTERPRISE.value,
            )
            
            if not integration:
                logger.warning(
                    f"GitHub Enterprise integration not found: integration_id={integration_id}, "
                    f"organization_id={organization_id}"
                )
                return {"success": False, "error": "Integration not found"}
            
            # Verify the integration belongs to the specified organization
            org_integration = integration_service.get_organization_integration(
                integration_id=integration.id,
                organization_id=organization_id,
            )
            
            if not org_integration:
                logger.warning(
                    f"GitHub Enterprise integration not found for organization: "
                    f"integration_id={integration_id}, organization_id={organization_id}"
                )
                return {"success": False, "error": "Integration not found for organization"}
            
            # Check if integration is active
            if integration.status != ObjectStatus.ACTIVE:
                logger.warning(
                    f"GitHub Enterprise integration is not active: "
                    f"integration_id={integration_id}, status={integration.status}"
                )
                return {"success": False, "error": "Integration is not active"}
            
            # Extract configuration from integration metadata
            metadata = integration.metadata or {}
            
            config = {
                "success": True,
                "integration_id": str(integration.id),
                "external_id": integration.external_id,
                "base_url": metadata.get("base_url"),
                "api_url": metadata.get("api_url"),
                "installation_id": integration.external_id,
                "app_id": metadata.get("app_id"),
                "domain_name": metadata.get("domain_name"),
                "name": integration.name,
                "provider": integration.provider,
            }
            
            logger.info(
                f"Successfully retrieved GitHub Enterprise integration config: "
                f"integration_id={integration_id}, organization_id={organization_id}"
            )
            
            return config
            
        except ValueError as e:
            logger.error(
                f"Invalid integration_id format: {integration_id}, error: {e}"
            )
            return {"success": False, "error": "Invalid integration ID format"}
            
        except Exception as e:
            logger.exception(
                f"Unexpected error retrieving GitHub Enterprise integration config: "
                f"integration_id={integration_id}, organization_id={organization_id}, error: {e}"
            )
            return {"success": False, "error": "Internal server error"}

    def _dispatch_to_local_method(self, method_name: str, arguments: dict):
        """
        Dispatch RPC method calls to local methods.
        """
        if method_name == "get_github_enterprise_integration_config":
            return self.get_github_enterprise_integration_config(**arguments)
        
        # Handle other RPC methods...
        return super()._dispatch_to_local_method(method_name, arguments)