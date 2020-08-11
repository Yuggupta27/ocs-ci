import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version, ManageTest, tier1
)
from ocs_ci.ocs.resources import pod
from tests import helpers

logger = logging.getLogger(__name__)


@tier1
@skipif_ocs_version('<4.6')
@pytest.mark.parametrize(
    argnames=["interface_type"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("")
        )
    ]
)
class TestPvcToPvcClone(ManageTest):
    """
    Tests to verify PVC to PVC clone feature
    """
    @pytest.fixture(autouse=True)
    def setup(self, interface_type, storageclass_factory, pvc_factory, pod_factory):
        """
        create resources for the test
        Args:
            interface_type(str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            storageclass_factory: A fixture to creare new storage class
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod
        """
        self.sc_obj = storageclass_factory(interface=interface_type)
        self.pvc_obj = pvc_factory(
            interface=interface_type,
            size=1,
            status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface_type,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING
        )

    def test_pvc_to_pvc_clone(self, interface_type, pod_factory):
        """
        Create a clone from an existing pvc,
        verify data is preserved in the cloning.
        """
        mountPath = "/var/lib/www/html/"
        file_name = "test_clone"
        test_file = mountPath + file_name

        logger.info(f"Running IO on pod {self.pod_obj.name}")
        logger.info(file_name)
        self.pod_obj.exec_cmd_on_pod(command=f"touch {test_file}")

        # Verify presence of the file
        assert pod.check_file_existence(self.pod_obj, test_file), (
            f"File {file_name} doesn't exist"
        )
        logger.info(f"File {file_name} exists in {self.pod_obj.name}")

        # Create a cloned pvc from the parent pvc
        cloned_pvc_obj = helpers.create_pvc_clone(
            sc_name=self.sc_obj.name,
            parent_pvc_name=self.pvc_obj.name
        )

        helpers.wait_for_resource_state(cloned_pvc_obj, constants.STATUS_BOUND)
        cloned_pvc_obj.reload()

        # Create and attach pod to the pvc
        clone_pod_obj = pod_factory(
            interface=interface_type,
            pvc=cloned_pvc_obj,
            status=constants.STATUS_RUNNING
        )

        # Verify file's presence on the new pod
        logger.info(f"Checking the existence of {file_name} on cloned pod {clone_pod_obj.name}")
        assert pod.check_file_existence(clone_pod_obj, test_file), (
            f"File {file_name} doesn't exist"
        )
        logger.info(f"File {file_name} exists in {clone_pod_obj.name}")

        # Delete the pod using parent pvc
        assert self.pod_obj.delete()
        self.pod_obj.ocp.wait_for_delete(resource_name=self.pod_obj.name)

        # Delete the pod using the cloned pvc
        assert clone_pod_obj.delete()
        clone_pod_obj.ocp.wait_for_delete(resource_name=clone_pod_obj.name)

        # Delete the parent pvc
        assert self.pvc_obj.delete()
        self.pvc_obj.ocp.wait_for_delete(resource_name=self.pvc_obj.name)

        # Delete the cloned pvc
        assert cloned_pvc_obj.delete()
        cloned_pvc_obj.ocp.wait_for_delete(resource_name=cloned_pvc_obj.name)
