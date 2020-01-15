import json
from ansible.module_utils.common.collections import ImmutableDict
from ansible.parsing.dataloader import DataLoader
from ansible.vars.manager import VariableManager
from ansible.inventory.manager import InventoryManager
from ansible.playbook.play import Play
from ansible.executor.task_queue_manager import TaskQueueManager
from ansible.plugins.callback import CallbackBase
from ansible import context
import ansible.constants as C
import shutil
import yaml


class PlaybookRunner(object):
    class ResultCallback(CallbackBase):
        """A sample callback plugin used for performing an action as results come in

        If you want to collect all results into a single object for processing at
        the end of the execution, look into utilizing the ``json`` callback plugin
        or writing your own custom callback plugin
        """

        def v2_runner_on_ok(self, result, **kwargs):
            """Print a json representation of the result

            This method could store the result in an instance attribute for retrieval later
            """
            host = result._host
            print(json.dumps({host.name: result._result}, indent=4))

    def __init__(self):
        # since the API is constructed for CLI it expects certain options to always be set in the context object
        context.CLIARGS = ImmutableDict(connection='local', module_path=['/to/mymodules'], forks=10, become=None,
                                        become_method=None, become_user=None, check=False, diff=False)

        # initialize needed objects
        self.loader = DataLoader()  # Takes care of finding and reading yaml, json and ini files
        self.passwords = dict(vault_pass='secret')

        # Instantiate our ResultCallback for handling results as they come in. Ansible expects this to be one of its main display outlets
        self.results_callback = self.ResultCallback()

        # create inventory, use path to host config file as source or hosts in a comma separated string
        self.inventory = InventoryManager(loader=self.loader, sources='localhost,')

        # variable manager takes care of merging all the different sources to give you a unified view of variables available in each context
        self.variable_manager = VariableManager(loader=self.loader, inventory=self.inventory)

    def run(self, playbook_file):
        with open(playbook_file, 'r') as yaml_file:
            play_source = yaml.load(yaml_file)
        play = Play().load(play_source[0])

        # Run it - instantiate task queue manager, which takes care of forking and setting up all objects to iterate over host list and tasks
        tqm = None
        try:
            tqm = TaskQueueManager(
                inventory=self.inventory,
                variable_manager=self.variable_manager,
                loader=self.loader,
                passwords=self.passwords,
                stdout_callback=self.results_callback,
                # Use our custom callback instead of the ``default`` callback plugin, which prints to stdout
            )
            result = tqm.run(play)  # most interesting data for a play is actually sent to the callback's methods
        finally:
            # we always need to cleanup child procs and the structures we use to communicate with them
            if tqm is not None:
                tqm.cleanup()

            # Remove ansible tmpdir
            shutil.rmtree(C.DEFAULT_LOCAL_TMP, True)