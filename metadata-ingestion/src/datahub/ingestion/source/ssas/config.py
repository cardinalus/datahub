from typing import List

import datahub.emitter.mce_builder as builder
import pydantic
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvBasedSourceConfigBase


class SsasServerHTTPConfig(EnvBasedSourceConfigBase):
    username: str = pydantic.Field(description="Windows account username")
    password: str = pydantic.Field(description="Windows account password")
    workstation_name: str = pydantic.Field(
        default="", description="Workstation name"
    )
    host_port: str = pydantic.Field(
        default="", description=""
    )
    server_alias: str = pydantic.Field(default="")

    virtual_directory_name: str = pydantic.Field(
        default="ssas", description="Report Virtual Directory URL name"
    )

    instance: str = pydantic.Field(default="")
    catalog_name: str = pydantic.Field(default="")

    dns_suffixes: List = pydantic.Field(default_factory=list)

    @property
    def use_dns_resolver(self) -> bool:
        return bool(self.dns_suffixes)

    @property
    def get_base_api_http_url(self) -> str:
        return f"http://{self.host_port}/{self.virtual_directory_name}/{self.instance}/msmdpump.dll"

    @property
    def get_base_api_https_url(self) -> str:
        return f"https://{self.host_port}/{self.virtual_directory_name}/{self.instance}/msmdpump.dll"

    @property
    def host(self) -> str:
        return self.server_alias or self.host_port.split(":")[0]


class SsasServerHTTPSourceConfig(SsasServerHTTPConfig):
    platform_name: str = "ssas"
    platform_urn: str = builder.make_data_platform_urn(platform=platform_name)
    report_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    chart_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
