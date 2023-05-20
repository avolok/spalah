from delta import DeltaTable
from pyspark.sql import SparkSession
from typing import Union
from spalah.shared.logging import get_logger


logger = get_logger(__name__)


class DeltaProperty:
    """
    Manages Delta Table properties, constraints, etc.


    Attributes:
        keep_existing_properties (bool): Preserves existing table properties if they are not
                                                in the input value. Defaults to False
        keep_existing_check_constraints (bool): Preserves existing table constraints if they are not
                                                in the input value. Defaults to False
    """

    keep_existing_properties = False
    keep_existing_check_constraints = False

    def __init__(
        self,
        table_path: str = "",
        table_name: str = "",
        spark_session: SparkSession = None,
    ) -> None:
        """
        Args:
            table_path (str, optional): Path to delta table. For instance: /mnt/db1/table1
            table_name (str, optional): Delta table name. For instance: db1.table1
            spark_session: (SparkSession, optional)  The current spark context.
        Raises:
            ValueError: if values for both 'table_path' and 'table_name' provided
                        provide values to one of them
            ValueError: if values for neither 'table_path' nor 'table_name' provided
                        provide values to one of them
        Examples:
            >>> from spalah.datalake import DeltaProperty
            >>> dp = DeltaProperty(table_path="/path/dataset")
            >>> print(dp.properties)
            {'delta.deletedFileRetentionDuration': 'interval 15 days'}
        """

        self.spark_session = (
            SparkSession.getActiveSession() if not spark_session else spark_session
        )
        self.table_name = self.__get_table_identifier(
            table_path=table_path, table_name=table_name
        )
        self.original_table_name = table_name

    def __get_table_identifier(
        self,
        table_path: str = "",
        table_name: str = "",
    ) -> Union[str, None]:
        """Constructs table identifier from provided values."""

        if table_path and table_name:
            raise ValueError(
                "Both 'table_path' and 'table_name' provided. Use one of them."
            )

        if not table_path and not table_name:
            raise ValueError(
                "Neither 'table_path' nor 'table_name' provided. Use one of them."
            )

        if table_path:
            table_name = f"delta.`{table_path}`"
            _identifier = table_path
        else:
            _identifier = table_name

        if not DeltaTable.isDeltaTable(
            sparkSession=self.spark_session, identifier=_identifier
        ):
            logger.warning(f"{table_name} is not a Delta Table")
            return None

        return table_name

    @property
    def properties(self) -> Union[dict, None]:
        """Gets/sets dataset's delta table properties.

        Args:
            value (dict):  An input dictionary in the format: `{"property_name": "value"}`

        Examples:
            >>> from spalah.datalake import DeltaProperty
            >>> dp = DeltaProperty(table_path="/path/dataset")
            >>>
            >>> # get existing properties
            >>> print(dp.properties)
            {'delta.deletedFileRetentionDuration': 'interval 15 days'}
            >>>
            >>> # Adjust the property value from 15 to 30 days
            >>> dp.properties = {'delta.deletedFileRetentionDuration': 'interval 30 days'}
        """

        if self.table_name:
            existing_properties = (
                self.spark_session.sql(f"DESCRIBE DETAIL {self.table_name}")
                .select("properties")
                .collect()[0]
                .asDict()["properties"]
            )
        else:
            existing_properties = None

        return existing_properties

    @properties.setter
    def properties(
        self,
        value: dict,
    ) -> None:
        """Sets delta properties

        Args:
            value (dict):  An input dictionary in the format: `{"property_name": "value"}`
        """

        _existing_properties = self.properties
        _new_properties = value

        if self.table_name:
            logger.info(f"Applying table properties on '{self.table_name}':")

            for k, v in _new_properties.items():
                logger.info(f"Checking if '{k} = {v}' is set on {self.table_name}")

                if k in _existing_properties and _existing_properties[k] == str(v):
                    logger.info("The property already exists on the table")
                else:
                    _sql = (
                        f"ALTER TABLE {self.table_name} SET TBLPROPERTIES ({k} = '{v}')"
                    )
                    self.spark_session.sql(_sql)
                    logger.info("The property has been set")

            if not self.keep_existing_properties:
                for k, v in _existing_properties.items():
                    if k not in _new_properties:
                        _sql = (
                            f"ALTER TABLE {self.table_name} UNSET TBLPROPERTIES ({k})"
                        )
                        self.spark_session.sql(_sql)
                        logger.info(
                            f"The property '{k} = {v}' has been unset because it is not defined in "
                            "the original dict"
                        )

    @property
    def check_constraints(self) -> Union[dict, None]:
        """Gets/sets dataset's delta table check constraints.

        Args:
            value (dict):  An input dictionary in the format: `{"property_name": "value"}`

        Examples:
            >>> from spalah.datalake import DeltaProperty
            >>> dp = DeltaProperty(table_path="/path/dataset")
            >>>
            >>> # get existing constraints
            >>> print(dp.check_constraints)
            {}
            >>>
            >>> # Add a new check constraint
            >>> dp.check_constraints = {'id_is_not_null': 'id is not null'}
        """

        _constraints = {}

        if self.table_name:
            for k, v in self.properties.items():
                if k.startswith("delta.constraints."):
                    _new_key = k.replace("delta.constraints.", "")
                    _constraints[_new_key] = v
        else:
            _constraints = None

        return _constraints

    @check_constraints.setter
    def check_constraints(
        self,
        value: dict,
    ) -> None:
        """Dataset's delta table check constraints setter method.

        Args:
            value (dict): inptut dictionary in the format:
                            `{"constraint_name": "constraint definition"}`
        """

        if self.table_name:
            _existing_constraints = self.check_constraints
            _new_constraints = value

            logger.info(f"Applying check constraints on '{self.table_name}':")

            if not self.keep_existing_check_constraints:
                for k, v in _existing_constraints.items():
                    if k not in _new_constraints:
                        _sql = f"ALTER TABLE {self.table_name} DROP CONSTRAINT {k}"
                        self.spark_session.sql(_sql)
                        logger.info(
                            f"The constraint '{k}' has been dropped from the "
                            f"table '{self.table_name}'"
                        )

            for k, v in _new_constraints.items():
                _constraint_name = k.lower()
                logger.info(
                    f"Checking if constraint '{_constraint_name}' was "
                    f"already set on {self.table_name}"
                )

                if _constraint_name in _existing_constraints:
                    logger.info(
                        f"The constraint '{_constraint_name}' already exists on the table"
                    )
                else:
                    if v in _existing_constraints.values():
                        logger.warning(
                            f" The constraint definition '{v}' already present on the table"
                        )
                    else:
                        _sql = (
                            f"ALTER TABLE {self.table_name} ADD CONSTRAINT "
                            f"{_constraint_name} CHECK ({v})"
                        )

                        self.spark_session.sql(_sql)
                        logger.info(
                            f"The constraint {_constraint_name} has been successfully "
                            f"added to '{self.table_name}'"
                        )
