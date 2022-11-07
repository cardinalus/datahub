"""
Module for dao layer of tabular ms ssas.
"""

from typing import Any, Dict, List

from requests.auth import HTTPBasicAuth

from ssas.api import ISsasAPI, SsasXmlaAPI
from ssas.config import SsasServerHTTPConfig, SsasServerHTTPSourceConfig
from ssas.parser import MdXmlaParser
from ssas.xmlaclient import XmlaClient

from .domains import XMLACube, XMLADataBase, XMLADimension, XMLAMeasure, XMLAServer
from .tools import DvmQueries


class SsasDvmAPI(ISsasAPI):
    """
    API endpoints to fetch catalogs, cubes, dimension, measures.
    """

    def __init__(self, config: SsasServerHTTPConfig, auth: HTTPBasicAuth) -> None:
        self.__config: SsasServerHTTPConfig = config
        self.__auth = auth
        self.__client = XmlaClient(config=self.__config, auth=self.__auth)

    def get_server(self):
        pass

    def get_catalogs(self):
        """
        Get list catalogs from dvm query.

        :return: list catalogs.
        """
        response = self.__client.execute(query=DvmQueries.SELECT_CATALOGS)
        return response.get_node()

    def get_cubes_by_catalog(self, catalog: str):
        """
        Get  list catalogs from dvm query.

        :return: list catalogs.
        """

        response = self.__client.execute(
            query=DvmQueries.SELECT_CUBES_BY_CATALOG.format(catalog=catalog),  # type: ignore
            catalog_name=catalog,
        )
        return response.get_node()

    def get_dimensions_by_cube(self, catalog_name: str, cube_name: str):
        """
        Get dimension from dvm query.

        :return: dimension as dict.
        """

        response = self.__client.execute(
            query=DvmQueries.SELECT_DIMENSIONS_BY_CUBE.format(name=cube_name),  # type: ignore
            catalog_name=catalog_name,
        )
        return response.get_node()

    def get_dimensions_additional_info(self, catalog_name: str, dimension_name: str):
        """
        Get dimension additional info from dvm query.

        :return: dimension additional info as dict.
        """

        response = self.__client.execute(
            query=DvmQueries.SELECT_QUERY_DEFINITION.format(name=dimension_name),
            catalog_name=catalog_name,
        )
        return response.get_node()

    def get_measures_by_cube(self, catalog_name: str, cube_name: str):
        """
        Get measure from dvm query.

        :return: measure as dict.
        """
        response = self.__client.execute(
            query=DvmQueries.SELECT_MEASURES_BY_CUBE.format(name=cube_name),  # type: ignore
            catalog_name=catalog_name,
        )
        return response.get_node()

    @property
    def auth_credentials(self):
        """
        Get authorization credentials.

        :return: authorization dataclass.
        """
        return self.__auth


class SsasTabularDvmAPI(SsasDvmAPI):
    """
    API endpoints to fetch catalogs, cubes, dimension, measures for tabular ssas servers.
    """

    def get_catalog_sources(self) -> Dict[str, Any]:
        """
        Get list catalogs from dvm query.

        :return: list catalogs.
        """

        response = self.__client.execute(query=DvmQueries.SELECT_DATA_SOURCES)
        return response.as_dict()


class TabularSsasAPI(ISsasAPI):
    """
    API endpoints to fetch catalogs, cubes, dimension, measures for tabular ssas servers.
    """

    def __init__(self, config: SsasServerHTTPSourceConfig):
        self.__config = config
        self.__auth = HTTPBasicAuth(
            username=self.__config.username,
            password=self.__config.password,
        )
        self.__xmla_api = SsasXmlaAPI(config=self.__config, auth=self.__auth)
        self.__dvm_api = SsasTabularDvmAPI(config=self.__config, auth=self.__auth)
        self.__xmla_parser = MdXmlaParser()

    def get_server(self) -> XMLAServer:
        """
        Get server metadata.

        :return: structured metadata dataclass.
        """

        xmla_data = self.__xmla_api.get_server_metadata()
        return XMLAServer(**self.__xmla_parser.get_server(xmla_data))

    def get_catalogs(self) -> List[XMLADataBase]:
        """
        Get list catalogs from dvm query.

        :return: list catalogs.
        """

        return [XMLADataBase(**item) for item in self.__dvm_api.get_catalogs()]

    def get_cubes_by_catalog(self, catalog: str) -> List[XMLACube]:
        """
        Get  list catalogs from dvm query.

        :return: list catalogs.
        """

        return [
            XMLACube(**item)
            for item in self.__dvm_api.get_cubes_by_catalog(catalog=catalog)
        ]

    def add_dimension_additional_info(
        self, catalog_name: str, dimension: XMLADimension
    ):
        """
        Add additional info to dimension.

        :return: dimension.
        """
        dimension_name = dimension.name
        if dimension_name is None:
            return dimension

        info = self.__dvm_api.get_dimensions_additional_info(
            dimension_name=dimension_name, catalog_name=catalog_name
        )

        for item in info:
            dimension.query_definition = item["QueryDefinition"]

        return dimension

    def get_dimensions_by_cube(
        self, catalog_name: str, cube_name: str
    ) -> List[XMLADimension]:
        """
        Get dimension from cube.

        :return: dimension.
        """

        result = []

        dimensions = [
            XMLADimension(**item)
            for item in self.__dvm_api.get_dimensions_by_cube(
                catalog_name=catalog_name, cube_name=cube_name
            )
        ]

        for dimension in dimensions:
            dimension = self.add_dimension_additional_info(
                catalog_name=catalog_name, dimension=dimension
            )
            result.append(dimension)

        return result

    def get_measures_by_cube(
        self, catalog_name: str, cube_name: str
    ) -> List[XMLAMeasure]:
        """
        Get measure from cube.

        :return: measure.
        """
        return [
            XMLAMeasure(**item)
            for item in self.__dvm_api.get_measures_by_cube(
                catalog_name=catalog_name, cube_name=cube_name
            )
        ]

    @property
    def auth_credentials(self) -> HTTPBasicAuth:
        """
        Get authorization credentials.

        :return: authorization dataclass.
        """

        return self.__auth

    def get_server_info(self) -> Dict[str, Any]:
        """
        Get server metadata from XMLA request.

        :return: structured metadata as dict.
        """

        return self.__xmla_api.get_server_info()
