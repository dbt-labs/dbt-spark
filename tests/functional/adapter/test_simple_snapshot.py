from dbt.tests.adapter.simple_snapshot.test_snapshot import BaseSnapshotCheck, BaseSimpleSnapshot

from dbt.tests.adapter.simple_snapshot.test_various_configs import (
    BaseSnapshotColumnNames,
    BaseSnapshotColumnNamesFromDbtProject,
    BaseSnapshotInvalidColumnNames,
    BaseSnapshotDbtValidToCurrent,
)


class TestSnapshot(BaseSimpleSnapshot):
    pass


class TestSnapshotCheck(BaseSnapshotCheck):
    pass


class TestSnapshotColumnNames(BaseSnapshotColumnNames):
    pass


class TestSnapshotColumnNamesFromDbtProject(BaseSnapshotColumnNamesFromDbtProject):
    pass


class TestSnapshotInvalidColumnNames(BaseSnapshotInvalidColumnNames):
    pass


class TestSnapshotDbtValidToCurrent(BaseSnapshotDbtValidToCurrent):
    pass
