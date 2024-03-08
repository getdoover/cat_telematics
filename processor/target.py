#!/usr/bin/python3
from operator import truediv
import os, traceback, sys, time, json, math
from signal import signal


## This is the definition for a tiny lambda function
## Which is run in response to messages processed in Doover's 'Channels' system

## In the doover_config.json file we have defined some of these subscriptions
## These are under 'processor_deployments' > 'tasks'

## You can import the pydoover module to interact with Doover based on decisions made in this function
## Just add the current directory to the path first
# sys.path.append(os.path.dirname(__file__))
import sys

## attempt to delete any loaded pydoover modules that persist across lambdas
if 'pydoover' in sys.modules:
    del sys.modules['pydoover']
try: del pydoover
except: pass
try: del pd
except: pass

import pydoover as pd
from cat_api_iface import cat_api_iface


class target:

    def __init__(self, *args, **kwargs):

        self.kwargs = kwargs
        ### kwarg
        #     'agent_id' : The Doover agent id invoking the task e.g. '9843b273-6580-4520-bdb0-0afb7bfec049'
        #     'access_token' : A temporary token that can be used to interact with the Doover API .e.g 'ABCDEFGHJKLMNOPQRSTUVWXYZ123456890',
        #     'api_endpoint' : The API endpoint to interact with e.g. "https://my.doover.com",
        #     'package_config' : A dictionary object with configuration for the task - as stored in the task channel in Doover,
        #     'msg_obj' : A dictionary object of the msg that has invoked this task,
        #     'task_id' : The identifier string of the task channel used to run this processor,
        #     'log_channel' : The identifier string of the channel to publish any logs to


    ## This function is invoked after the singleton instance is created
    def execute(self):

        start_time = time.time()

        self.create_doover_client()

        self.uplink_recv_channel = pd.channel(
            api_client=self.cli.api_client,
            agent_id=self.kwargs['agent_id'],
            channel_name='uplink_recv',
        )

        self.add_to_log( "running : " + str(os.getcwd()) + " " + str(__file__) )

        self.add_to_log( "kwargs = " + str(self.kwargs) )
        self.add_to_log( str( start_time ) )

        try:
            
            ## Do any processing you would like to do here
            message_type = None
            if 'message_type' in self.kwargs['package_config'] and 'message_type' is not None:
                message_type = self.kwargs['package_config']['message_type']

            if message_type == "DEPLOY":
                self.deploy()

            if message_type == "DOWNLINK":
                self.downlink()

            if message_type == "UPLINK":
                self.uplink()

            if message_type == "FETCH":
                self.fetch()

        except Exception as e:
            self.add_to_log("ERROR attempting to process message - " + str(e))
            self.add_to_log(traceback.format_exc())

        self.complete_log()

    def deploy(self):
        ## Run any deployment code here
        
        ## Get the deployment channel
        ui_state_channel = self.cli.get_channel(
            channel_name="ui_state",
            agent_id=self.kwargs['agent_id']
        )

        ui_obj = {
            "state" : {
                "type": "uiContianer",
                "displayString": "",
                "children": {
                    "significantEvent": {
                        "type": "uiAlertStream",
                        "name": "significantEvent",
                        "displayString": "Notify me of any problems"
                    },
                    
                    "node_connection_info": {
                        "type": "uiConnectionInfo",
                        "name": "node_connection_info",
                        "connectionType": "periodic",
                        "connectionPeriod": 600,
                        "nextConnection": 600
                    }
                }
            }
        }

        self.add_to_log("deploying ui state " + str(ui_obj))

        ui_state_channel.publish(
            msg_str=json.dumps(ui_obj)
        )

    def uplink(self):
        ## Run any uplink processing code here
        
        ## Get the deployment channel
        ui_state_channel = self.cli.get_channel(
            channel_name="ui_state",
            agent_id=self.kwargs['agent_id']
        )

        ## Get the deployment channel
        ui_cmds_channel = self.cli.get_channel(
            channel_name="ui_cmds",
            agent_id=self.kwargs['agent_id']
        )

        self.compute_output_levels(ui_cmds_channel, ui_state_channel)
        self.update_reported_signal_strengths(ui_cmds_channel, ui_state_channel)

        ui_state_channel.update() ## Update the details stored in the state channel so that warnings are computed from current values
        self.assess_warnings(ui_cmds_channel, ui_state_channel)


    def downlink(self):
        ## Run any downlink processing code here
        
        self.send_uplink_interval_if_required()
        self.send_burst_mode_if_required()

    def fetch(self):
        ## Run any fetch processing code here
        # 
        try:
            success = self.get_cat_keys()
        except Exception as e:
            self.add_to_log("ERROR could not retrieve cat API keys from deployment config " + str(e))
        
        try:
            success = self.get_machine_details() and success
        except Exception as e:  
            self.add_to_log("ERROR could not retrieve machine serial number from deployment config " + str(e)) 
        
        if success:
            self.cat_api_iface = cat_api_iface(
                key_id = self.cat_key_id,
                key_secret = self.cat_key_secret,
            )

            try:
                msg = self.cat_api_iface.get_equipment_overview(self.machine_make, self.machine_model, self.machine_serial_number)
            except Exception as e:
                self.add_to_log("ERROR could not retrieve equipment overview from cat API " + str(e))

            if msg is not None:
                self.uplink_recv_channel.publish(
                    msg_str=msg
                )

    def get_cat_keys(self):
        # retrieve cat key id and key secret from the deployment config
        if self.kwargs['agent_settings'] is not None and 'deployment_config' in self.kwargs['agent_settings'] and self.kwargs['agent_settings']['deployment_config'] is not None:
            deployment_config = self.kwargs['agent_settings']['deployment_config']
            if 'cat_api_key' in deployment_config and 'cat_api_secret' in deployment_config:
                self.cat_key_id = deployment_config['cat_api_id']
                self.cat_key_secret = deployment_config['cat_api_secret']
                self.add_to_log("cat keys retrieved" + str(self.cat_key_id) + " " + str(self.cat_key_secret))
                return True
        return False
    
    def get_machine_details(self):
        if self.kwargs['agent_settings'] is not None and 'deployment_config' in self.kwargs['agent_settings'] and self.kwargs['agent_settings']['deployment_config'] is not None:
            deployment_config = self.kwargs['agent_settings']['deployment_config']
            if 'serial_number' and '' and '' in deployment_config:
                self.machine_serial_number = deployment_config['machine_serial_number']
                self.machine_model = deployment_config['machine_model']
                self.machine_make = deployment_config['machine_make']
                self.add_to_log("machine details retrieved" + str(self.machine_serial_number) + " " + str(self.machine_model) + " " + str(self.machine_make))
                return True
        return False
            
    def add_to_log(self, msg):
        if not hasattr(self, '_log'):
            self._log = ""
        self._log = self._log + str(msg) + "\n"

    def complete_log(self):
        if hasattr(self, '_log') and self._log is not None:
            log_channel = self.cli.get_channel( channel_id=self.kwargs['log_channel'] )
            log_channel.publish(
                msg_str=self._log
            )

    def create_doover_client(self):
        self.cli = pd.doover_iface(
            agent_id=self.kwargs['agent_id'],
            access_token=self.kwargs['access_token'],
            endpoint=self.kwargs['api_endpoint'],
        )