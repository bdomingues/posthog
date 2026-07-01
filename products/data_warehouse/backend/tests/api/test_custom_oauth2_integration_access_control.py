import uuid

import pytest
from posthog.test.base import APIBaseTest
from unittest.mock import patch

from parameterized import parameterized
from rest_framework import status

from posthog.constants import AvailableFeature
from posthog.models.organization import OrganizationMembership
from posthog.models.user import User

from products.warehouse_sources.backend.facade.models import CustomOAuth2Integration, ExternalDataSource

try:
    from ee.models.rbac.access_control import AccessControl
except ImportError:
    pass

VIEW_MODULE = "products.data_warehouse.backend.presentation.views.custom_oauth2_integration"


@pytest.mark.ee
@patch(f"{VIEW_MODULE}.is_url_allowed", return_value=(True, None))
class TestCustomOAuth2IntegrationAccessControl(APIBaseTest):
    """The viewset scopes as `external_data_source` but serves CustomOAuth2Integration rows, which aren't in
    the RBAC model→resource map. These cover that the integration's authorization is delegated to the source
    it backs, so a member with access to one source can't reach another restricted source's integration."""

    def setUp(self):
        super().setUp()
        self.organization.available_product_features = [
            {"key": AvailableFeature.ACCESS_CONTROL, "name": AvailableFeature.ACCESS_CONTROL},
            {"key": AvailableFeature.ROLE_BASED_ACCESS, "name": AvailableFeature.ROLE_BASED_ACCESS},
        ]
        self.organization.save()

        self.member = User.objects.create_and_join(self.organization, "member@posthog.com", "testtest")
        self.allowed_source = self._make_source("allowed")
        self.restricted_source = self._make_source("restricted")
        self.allowed_integration = self._make_integration(self.allowed_source)
        self.restricted_integration = self._make_integration(self.restricted_source)

    def _url(self, suffix: str = "") -> str:
        return f"/api/projects/{self.team.pk}/custom_oauth2_integrations/{suffix}"

    def _make_source(self, name: str) -> ExternalDataSource:
        return ExternalDataSource.objects.create(
            team_id=self.team.pk,
            source_id=str(uuid.uuid4()),
            connection_id=str(uuid.uuid4()),
            destination_id=str(uuid.uuid4()),
            source_type="Custom",
            created_by=self.user,
            prefix=name,
        )

    def _make_integration(self, source: ExternalDataSource) -> CustomOAuth2Integration:
        return CustomOAuth2Integration.objects.for_team(self.team.pk).create(
            team=self.team,
            external_data_source=source,
            config={"client_id": "cid", "token_url": "https://auth.example.com/token"},
            sensitive_config={"client_secret": "s"},
        )

    def _grant(self, resource_id, access_level: str) -> None:
        membership = OrganizationMembership.objects.get(user=self.member, organization=self.organization)
        AccessControl.objects.create(
            team=self.team,
            resource="external_data_source",
            resource_id=resource_id,
            access_level=access_level,
            organization_member=membership,
        )

    def _restrict_member_to_allowed_source(self, level: str = "viewer") -> None:
        # Project-default none, plus explicit access to the allowed source only.
        self._grant(resource_id=None, access_level="none")
        self._grant(resource_id=str(self.allowed_source.id), access_level=level)

    def test_member_cannot_retrieve_integration_for_inaccessible_source(self, _mock):
        self._restrict_member_to_allowed_source()
        self.client.force_login(self.member)

        response = self.client.get(self._url(f"{self.restricted_integration.id}/"))

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_member_can_retrieve_integration_for_accessible_source(self, _mock):
        self._restrict_member_to_allowed_source()
        self.client.force_login(self.member)

        response = self.client.get(self._url(f"{self.allowed_integration.id}/"))

        assert response.status_code == status.HTTP_200_OK
        assert response.json()["id"] == str(self.allowed_integration.id)

    def test_list_excludes_integrations_for_inaccessible_sources(self, _mock):
        self._restrict_member_to_allowed_source()
        self.client.force_login(self.member)

        response = self.client.get(self._url())

        assert response.status_code == status.HTTP_200_OK
        assert {row["id"] for row in response.json()["results"]} == {str(self.allowed_integration.id)}

    @parameterized.expand(["patch", "delete"])
    def test_viewer_of_source_cannot_mutate_its_integration(self, _mock, verb: str):
        self._restrict_member_to_allowed_source(level="viewer")
        self.client.force_login(self.member)

        if verb == "delete":
            response = self.client.delete(self._url(f"{self.allowed_integration.id}/"))
        else:
            response = self.client.patch(
                self._url(f"{self.allowed_integration.id}/"), {"refresh_token": "stolen"}, format="json"
            )

        assert response.status_code == status.HTTP_403_FORBIDDEN
        # The secret was not rotated by the rejected request.
        reloaded = CustomOAuth2Integration.objects.for_team(self.team.pk).get(id=self.allowed_integration.id)
        assert "refresh_token" not in reloaded.sensitive_config

    def test_editor_of_source_can_reconnect_its_integration(self, _mock):
        self._restrict_member_to_allowed_source(level="editor")
        self.client.force_login(self.member)

        response = self.client.patch(
            self._url(f"{self.allowed_integration.id}/"), {"refresh_token": "new"}, format="json"
        )

        assert response.status_code == status.HTTP_200_OK

    def test_cannot_rebind_integration_to_inaccessible_source(self, _mock):
        # A PATCH that rebinds the integration onto a source the caller can't edit must be rejected — even
        # if they can edit the integration's current source — otherwise they could reserve it against a
        # restricted source.
        self._restrict_member_to_allowed_source(level="editor")
        target = self._make_source("rebind_target")  # no explicit grant → member can't edit it
        self.client.force_login(self.member)

        response = self.client.patch(
            self._url(f"{self.allowed_integration.id}/"),
            {"external_data_source": str(target.id)},
            format="json",
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN
        reloaded = CustomOAuth2Integration.objects.for_team(self.team.pk).get(id=self.allowed_integration.id)
        assert str(reloaded.external_data_source_id) == str(self.allowed_source.id)

    def test_unlinked_integration_visible_only_to_its_creator(self, _mock):
        # An unlinked integration is a floating credential: only its creator may see it, so a teammate can't
        # list its UUID and adopt it into a source they control.
        mine = CustomOAuth2Integration.objects.for_team(self.team.pk).create(
            team=self.team,
            created_by=self.member,
            config={"client_id": "x", "token_url": "https://auth.example.com/token"},
            sensitive_config={"client_secret": "s"},
        )

        # A different user can neither list nor retrieve the member's unlinked integration.
        self.client.force_login(self.user)
        listed = self.client.get(self._url())
        assert str(mine.id) not in {row["id"] for row in listed.json()["results"]}
        assert self.client.get(self._url(f"{mine.id}/")).status_code == status.HTTP_404_NOT_FOUND

        # Its creator sees it.
        self.client.force_login(self.member)
        assert self.client.get(self._url(f"{mine.id}/")).status_code == status.HTTP_200_OK
