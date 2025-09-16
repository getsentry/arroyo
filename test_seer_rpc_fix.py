# Test file for the silo architecture fix
# This would be added to tests/sentry/seer/endpoints/test_seer_rpc.py

import pytest
from unittest.mock import Mock, patch
from sentry.seer.endpoints.seer_rpc import SeerRpcServiceEndpoint
from sentry.types.integrations import IntegrationProviderSlug
from sentry.constants import ObjectStatus


class TestSeerRpcServiceEndpointFix:
    """
    Tests for the fixed get_github_enterprise_integration_config method
    """
    
    def setup_method(self):
        self.endpoint = SeerRpcServiceEndpoint()
        
    @patch('sentry.services.hybrid_cloud.integration.integration_service')
    def test_get_github_enterprise_integration_config_success(self, mock_integration_service):
        """Test successful retrieval of GitHub Enterprise integration config"""
        
        # Mock integration object
        mock_integration = Mock()
        mock_integration.id = 261546
        mock_integration.external_id = "12345"
        mock_integration.name = "GitHub Enterprise"
        mock_integration.provider = IntegrationProviderSlug.GITHUB_ENTERPRISE.value
        mock_integration.status = ObjectStatus.ACTIVE
        mock_integration.metadata = {
            "base_url": "https://github.example.com",
            "api_url": "https://github.example.com/api/v3",
            "app_id": "12637",
            "domain_name": "github.example.com"
        }
        
        # Mock organization integration
        mock_org_integration = Mock()
        mock_org_integration.id = 1
        
        # Setup mock service calls
        mock_integration_service.get_integration.return_value = mock_integration
        mock_integration_service.get_organization_integration.return_value = mock_org_integration
        
        # Call the method
        result = self.endpoint.get_github_enterprise_integration_config(
            integration_id="261546",
            organization_id=1118521
        )
        
        # Verify the result
        assert result["success"] is True
        assert result["integration_id"] == "261546"
        assert result["external_id"] == "12345"
        assert result["base_url"] == "https://github.example.com"
        assert result["api_url"] == "https://github.example.com/api/v3"
        assert result["app_id"] == "12637"
        assert result["domain_name"] == "github.example.com"
        assert result["name"] == "GitHub Enterprise"
        assert result["provider"] == IntegrationProviderSlug.GITHUB_ENTERPRISE.value
        
        # Verify service calls
        mock_integration_service.get_integration.assert_called_once_with(
            integration_id=261546,
            provider=IntegrationProviderSlug.GITHUB_ENTERPRISE.value,
        )
        mock_integration_service.get_organization_integration.assert_called_once_with(
            integration_id=261546,
            organization_id=1118521,
        )
    
    @patch('sentry.services.hybrid_cloud.integration.integration_service')
    def test_get_github_enterprise_integration_config_not_found(self, mock_integration_service):
        """Test handling when integration is not found"""
        
        # Mock service to return None (integration not found)
        mock_integration_service.get_integration.return_value = None
        
        # Call the method
        result = self.endpoint.get_github_enterprise_integration_config(
            integration_id="999999",
            organization_id=1118521
        )
        
        # Verify error response
        assert result["success"] is False
        assert result["error"] == "Integration not found"
    
    @patch('sentry.services.hybrid_cloud.integration.integration_service')
    def test_get_github_enterprise_integration_config_org_not_found(self, mock_integration_service):
        """Test handling when integration exists but not for the organization"""
        
        # Mock integration exists but org integration doesn't
        mock_integration = Mock()
        mock_integration.id = 261546
        mock_integration_service.get_integration.return_value = mock_integration
        mock_integration_service.get_organization_integration.return_value = None
        
        # Call the method
        result = self.endpoint.get_github_enterprise_integration_config(
            integration_id="261546",
            organization_id=999999
        )
        
        # Verify error response
        assert result["success"] is False
        assert result["error"] == "Integration not found for organization"
    
    @patch('sentry.services.hybrid_cloud.integration.integration_service')
    def test_get_github_enterprise_integration_config_inactive(self, mock_integration_service):
        """Test handling when integration is inactive"""
        
        # Mock inactive integration
        mock_integration = Mock()
        mock_integration.id = 261546
        mock_integration.status = ObjectStatus.DISABLED
        mock_integration_service.get_integration.return_value = mock_integration
        
        mock_org_integration = Mock()
        mock_integration_service.get_organization_integration.return_value = mock_org_integration
        
        # Call the method
        result = self.endpoint.get_github_enterprise_integration_config(
            integration_id="261546",
            organization_id=1118521
        )
        
        # Verify error response
        assert result["success"] is False
        assert result["error"] == "Integration is not active"
    
    def test_get_github_enterprise_integration_config_invalid_id(self):
        """Test handling invalid integration ID format"""
        
        # Call the method with invalid integration ID
        result = self.endpoint.get_github_enterprise_integration_config(
            integration_id="invalid",
            organization_id=1118521
        )
        
        # Verify error response
        assert result["success"] is False
        assert result["error"] == "Invalid integration ID format"
    
    @patch('sentry.services.hybrid_cloud.integration.integration_service')
    def test_get_github_enterprise_integration_config_exception(self, mock_integration_service):
        """Test handling unexpected exceptions"""
        
        # Mock service to raise an exception
        mock_integration_service.get_integration.side_effect = Exception("Database error")
        
        # Call the method
        result = self.endpoint.get_github_enterprise_integration_config(
            integration_id="261546",
            organization_id=1118521
        )
        
        # Verify error response
        assert result["success"] is False
        assert result["error"] == "Internal server error"