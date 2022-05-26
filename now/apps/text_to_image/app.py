from typing import Dict, List

from docarray import DocumentArray

from now.apps.base.app import JinaNOWApp


class text_to_image(JinaNOWApp):
    @property
    def get_flow_yaml(self) -> str:
        pass

    @property
    def get_options(self) -> List[Dict]:
        return [Options.quality]

    def setup(self, da: DocumentArray, user_config: Dict) -> Dict:
        """
        Runs before the flow is deployed.
        Common use cases:
            - create a database
            - finetune a model + push the artifact
            - notify other services
        :param da:
        :param user_config: user configuration based on the given options
        :return: dict used to replace variables in flow yaml and to clean up resources after the flow is terminated
        """
        pass

    def cleanup(self, app_config: dict) -> None:
        """
        Runs after the flow is terminated.
        Cleans up the resources created during setup.
        Common examples are:
            - delete a database
            - remove artifact
            - notify other services
        :param app_config: contains all information needed to clean up the allocated resources
        """
        pass
