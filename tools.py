#
# Copyright (c) 2024 Diego L. Pedro <diegolpedro@gmail.com>.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
import logging
import os

logger = logging.getLogger(__name__)

# Azure secret client
def get_azure_secret_client():

    # Datos de autenticacion
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    vault_url = os.getenv("AZURE_VAULT_URL")

    # Autenticación usando Client ID, Client Secret y Tenant ID
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )

    client = SecretClient(vault_url=vault_url, credential=credential)
    return client

# Azure blob client
def get_azure_blob_client(container_name, blob_name):

    # Azure Clob Secrets
    azs_client = get_azure_secret_client()
    account_name = azs_client.get_secret('blob-account-name').value
    account_key = azs_client.get_secret('blob-account-key').value

    # Create a BlobServiceClient for Azure Blob Service
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string)

    # Create a container if it doesn't exist
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception as e:
        logger.info(f'Container existente')

    # Create a blob client for an append blob
    blob_client = container_client.get_blob_client(blob_name)
    try:
        blob_client.get_blob_properties()
        logger.info(f'Blob existente')
    except ResourceNotFoundError:
        blob_client.create_append_blob()
    return blob_client