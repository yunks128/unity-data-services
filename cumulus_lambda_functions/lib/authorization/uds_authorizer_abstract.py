from abc import ABC, abstractmethod


class UDSAuthorizorAbstract(ABC):
    @abstractmethod
    def add_authorized_group(self, action: [str], resource: [str], tenant: str, venue: str, ldap_group_name: str):
        return

    @abstractmethod
    def delete_authorized_group(self, tenant: str, venue: str, ldap_group_name: str):
        return

    @abstractmethod
    def list_authorized_groups_for(self, tenant: str, venue: str):
        return

    @abstractmethod
    def update_authorized_group(self, action: [str], resource: [str], tenant: str, venue: str, ldap_group_name: str):
        return

    @abstractmethod
    def get_authorized_tenant(self, username: str, action: str, resource: str) -> list:
        return []
