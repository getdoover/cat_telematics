#!/usr/bin/python3
from operator import truediv
import os, traceback, sys, time, json, datetime, pytz, traceback, math
from signal import signal
from dateutil.relativedelta import relativedelta


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

if 'cat_api_iface_file' in sys.modules:
    del sys.modules['cat_api_iface_file']
try: del cat_api_iface
except: pass
try: del cat_api_iface_file
except: pass

from cat_api_iface_file import cat_api_iface


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

        self.notifications_channel = pd.channel(
            api_client=self.cli.api_client,
            agent_id=self.kwargs['agent_id'],
            channel_name='significantEvent',
        )

        self.activity_log_channel = pd.channel(
            api_client=self.cli.api_client,
            agent_id=self.kwargs['agent_id'],
            channel_name='activity_logs',
        )

        ## Get the state channel
        self.ui_state_channel = self.cli.get_channel(
            channel_name="ui_state",
            agent_id=self.kwargs['agent_id']
        )

        ## Get the cmds channel
        self.ui_cmds_channel = self.cli.get_channel(
            channel_name="ui_cmds",
            agent_id=self.kwargs['agent_id']
        )

        ## Get the location channel
        self.location_channel = self.cli.get_channel(
            channel_name="location",
            agent_id=self.kwargs['agent_id']
        )

        self.add_to_log( "running : " + str(os.getcwd()) + " " + str(__file__) )
        self.add_to_log( "kwargs = " + str(self.kwargs) )
        self.add_to_log( str( start_time ) )

        self.get_machine_details()

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
                    "location" : {
                        "type" : "uiVariable",
                        "varType" : "location",
                        "hide" : True,
                        "name" : "location",
                        "displayString" : "Location",
                    },
                    "deviceRunHours" : {
                        "type" : "uiVariable",
                        "varType" : "float",
                        "name" : "deviceRunHours",
                        "displayString" : "Engine Hours (hrs)",
                        "decPrecision": 2,
                    },
                    "deviceOdometer" : {
                        "type" : "uiVariable",
                        "varType" : "float",
                        "name" : "deviceOdometer",
                        "displayString" : "Machine Odometer (km)",
                        "decPrecision": 1,
                    },
                    "nextServiceEst" : {
                        "type" : "uiVariable",
                        "varType" : "text",
                        "name" : "nextServiceEst",
                        "displayString" : "Next Service Estimate",
                    },
                    "daysTillNextService" : {
                        "type" : "uiVariable",
                        "varType" : "float",
                        "name" : "daysTillNextService",
                        "displayString" : "Days To Next Service (days)",
                        "decPrecision": 0,
                    },
                    "smsServiceAlert": {
                        "type": "uiAlertStream",
                        "name": "significantEvent",
                        "displayString": ("Text me " + str(self.get_sms_alert_days()) + " days before next service"),
                    },
                    "hoursTillNextService" : {
                        "type" : "uiVariable",
                        "varType" : "float",
                        "name" : "hoursTillNextService",
                        "displayString" : "Hours To Next Service (hrs)",
                        "decPrecision": 1,
                    },
                    "kmsTillNextService" : {
                        "type" : "uiVariable",
                        "varType" : "float",
                        "name" : "kmsTillNextService",
                        "displayString" : "Kms Till Next Service (kms)",
                        "decPrecision": 1,
                    },
                    "aveHoursPerDay" : {
                        "type" : "uiVariable",
                        "varType" : "float",
                        "name" : "aveHoursPerDay",
                        "displayString" : "Ave Hours Per Day (hrs)",
                        "decPrecision": 1,
                    },
                    "aveKmsPerDay" : {
                        "type" : "uiVariable",
                        "varType" : "float",
                        "name" : "aveKmsPerDay",
                        "displayString" : "Ave Kms Per Day (kms)",
                        "decPrecision": 1,
                    },
                    "engineOn" : {
                        "type" : "uiVariable",
                        "varType" : "bool",
                        "name" : "engineOn",
                        "displayString" : "Engine On",
                    },
                    "maintenance_submodule": {
                        "type": "uiSubmodule",
                        "name": "maintenance_submodule",
                        "displayString": "Maintenance",
                        "children": {
                            "lastServiceDate" : {
                                "type" : "uiDatetimeParam",
                                "includeTime" : False,
                                "name" : "lastServiceDate",
                                "displayString" : "Last service done",
                            },
                            "lastServiceHours" : {
                                "type" : "uiFloatParam",
                                "min" : 0,
                                "name" : "lastServiceHours",
                                "displayString" : "At hours (hrs)",
                            },
                            "lastServiceOdo" : {
                                "type" : "uiFloatParam",
                                "min" : 0,
                                "name" : "lastServiceOdo",
                                "displayString" : "And at Odometer (kms)",
                            },
                            "serviceIntervalMonths" : {
                                "type" : "uiFloatParam",
                                "min" : 0,
                                "max" : 60,
                                "name" : "serviceIntervalMonths",
                                "displayString" : "Service Interval (months)",
                            },
                            "serviceIntervalHours" : {
                                "type" : "uiFloatParam",
                                "min" : 0,
                                "name" : "serviceIntervalHours",
                                "displayString" : "Service Interval (hrs)",
                            },
                            "serviceIntervalOdo" : {
                                "type" : "uiFloatParam",
                                "min" : 0,
                                "name" : "serviceIntervalOdo",
                                "displayString" : "Service Interval (kms)",
                            },
                            "nextServiceDue" : {
                                "type" : "uiVariable",
                                "varType" : "text",
                                "name" : "nextServiceDue",
                                "displayString" : "Next Service due (max)",
                            },
                            "nextServiceHours" : {
                                "type" : "uiVariable",
                                "varType" : "float",
                                "name" : "nextServiceHours",
                                "displayString" : "At hours (hrs)",
                            },
                            "nextServiceOdo" : {
                                "type" : "uiVariable",
                                "varType" : "float",
                                "name" : "nextServiceOdo",
                                "displayString" : "And at Odometer (kms)",
                            }
                        }
                    },
                    "config_submodule": {
                        "type": "uiSubmodule",
                        "name": "config_submodule",
                        "displayString": "Config",
                        "children": {
                            "setHours" : {
                                "type" : "uiFloatParam",
                                "name" : "setHours",
                                "displayString" : "Set Machine Hours (hrs)",
                            },
                            "setKms" : {
                                "type" : "uiFloatParam",
                                "name" : "setKms",
                                "displayString" : "Set Odometer (km)",
                            },
                            "warningSmsPeriod" : {
                                "type" : "uiFloatParam",
                                "name" : "warningSmsPeriod",
                                "displayString" : "SMS Alert Period (days)",
                            },
                            "aveCalcDays" : {
                                "type" : "uiFloatParam",
                                "name" : "aveCalcDays",
                                "displayString" : "Ave Use Calculation (days)",
                            }
                        }
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
        self.add_to_log("processing uplink ")
        uplink_aggregate = self.uplink_recv_channel.get_aggregate()
        self.add_to_log("uplink aggregate type is: " + str(type(uplink_aggregate)))
        
        ## Get the deployment channel
        got_machine_details = self.get_machine_details()
        check_uplink = self.check_uplink(uplink_aggregate)

        ##run checks for both the deployment config and that the uplink is for the correct device
        if got_machine_details is False or check_uplink is False:
            self.add_to_log("ERROR machine details not retrieved")
            return

        ## Get the location from the uplink aggregate
        location = None
        try:
            location = uplink_aggregate["Location"]
            self.add_to_log("location is: " + str(location)) 
            long = float(location["Longitude"])
            lat = float(location["Latitude"])
            alt = float(location["Altitude"])
        except Exception as e:
            self.add_to_log("ERROR could not retrieve location from uplink aggregate " + str(e))
        
        position = None
        if long is not None and lat is not None and alt is not None and location is not None:
            position = {
                            'lat': lat,
                            'long': long,
                            'alt': alt,
                        }
            self.location_channel.publish(
                msg_str=json.dumps(position)
            )

        ## Get the engine status from the uplink aggregate
        engine_on = None
        try:
            engine_on = uplink_aggregate["EngineStatus"]["Running"]
            self.add_to_log("engine on is: " + str(engine_on))
        except Exception as e:
            self.add_to_log("ERROR could not retrieve engine status from uplink aggregate " + str(e))
        
        if engine_on is not None:
                if not engine_on:
                    status_icon = "off"
                    display_string = "Off"
                else:
                    status_icon = None
                    display_string = "Running"

        ## Get the engine hours from the uplink aggregate
        engine_hours = None
        try:
            engine_hours = uplink_aggregate["CumulativeOperatingHours"]["Hour"]
            self.add_to_log("engine hours are: " + str(engine_hours))
        except Exception as e:
            self.add_to_log("ERROR could not retrieve engine hours from uplink aggregate " + str(e))
        
        ## Get the odometer from the uplink aggregate
        odometer = None
        try:
            odometer = uplink_aggregate["Distance"]["Odometer"]
            self.add_to_log("odometer is: " + str(odometer))
        except Exception as e:
            self.add_to_log("ERROR could not retrieve odometer from uplink aggregate " + str(e))

        self.ui_state_channel.publish(
            msg_str=json.dumps({
                "state" : {
                    "children" : {
                        "engineOn" : {
                            "currentValue" : engine_on
                        },
                        "deviceRunHours" : {
                            "currentValue" : engine_hours
                        },
                        "deviceOdometer" : {
                            "currentValue" : odometer
                        },
                        "location" : {
                            "currentValue" : position
                        },
                        "nextServiceEst" : {
                            "currentValue" : next_service_est
                        }
                    }
                }
            }),
            save_log=True
        )

        # self.compute_output_levels(ui_cmds_channel, ui_state_channel)
        # self.update_reported_signal_strengths(ui_cmds_channel, ui_state_channel)

        # ui_state_channel.update() ## Update the details stored in the state channel so that warnings are computed from current values
        # self.assess_warnings(ui_cmds_channel, ui_state_channel)

    def check_uplink(self, uplink_aggregate):
        if uplink_aggregate is None:
            self.add_to_log("ERROR no uplink aggregate")
            return False
        
        try:
            equipment_header = uplink_aggregate["EquipmentHeader"]
        except Exception as e:
            self.add_to_log("ERROR no equipment header in uplink aggregate " + str(e))
            return False

        try:
            make = equipment_header["OEMName"]
            model = equipment_header["Model"]
            serial = equipment_header["SerialNumber"]
        except Exception as e:
            self.add_to_log("ERROR could not retrieve make, model, serial from uplink aggregate " + str(e))
            return False
        
        if str(make) == str(self.machine_make) and str(model) == str(self.machine_model) and str(serial) == str(self.machine_serial_number):
            self.add_to_log("Machine details match uplink aggregate!")
            return True
        else:
            self.add_to_log("ERROR machine details do not match uplink aggregate")
            return False

    def downlink(self):
        ## Run any downlink processing code here
        self.add_to_log("downlink processing")
        # self.send_uplink_interval_if_required()
        # self.send_burst_mode_if_required()

    def fetch(self):
        ## Run any fetch processing code here
        # 
        self.add_to_log("fetching data from the fetch")
        try:
            self.add_to_log("retrieving cat keys")
            success = self.get_cat_keys()
            
        except Exception as e:
            self.add_to_log("ERROR could not retrieve cat API keys from deployment config " + str(e))
        
        try:
            self.add_to_log("retrieving machine details")
            success = self.get_machine_details() and success
        except Exception as e:  
            self.add_to_log("ERROR could not retrieve machine serial number from deployment config " + str(e)) 

        self.add_to_log("success var is " + str(success))
        if success is True:
            self.cat_api_iface = cat_api_iface(
                key_id = self.cat_key_id,
                key_secret = self.cat_key_secret,
            )
            self.add_to_log("cat api interface created")

            try:
                msg = self.cat_api_iface.get_equipment_overview(self.machine_make, self.machine_model, self.machine_serial_number)
                self.add_to_log("cat api response recieved")           
            except Exception as e:
                self.add_to_log("ERROR could not retrieve equipment overview from cat API " + str(e))

            self.add_to_log("cat api response " + str(msg))
            if msg is not None:
                self.uplink_recv_channel.publish(
                    msg_str=json.dumps(msg["Equipment"][0])
                )

    def get_cat_keys(self):
        # retrieve cat key id and key secret from the deployment config
        if self.kwargs['agent_settings'] is not None and 'deployment_config' in self.kwargs['agent_settings'] and self.kwargs['agent_settings']['deployment_config'] is not None:
            self.add_to_log("passed first test")
            deployment_config = self.kwargs['agent_settings']['deployment_config']
            if 'cat_api_id' in deployment_config and 'cat_api_secret' in deployment_config:
                self.cat_key_id = deployment_config['cat_api_id']
                self.cat_key_secret = deployment_config['cat_api_secret']
                self.add_to_log("cat keys retrieved" + str(self.cat_key_id) + " " + str(self.cat_key_secret))
                return True
        self.add_to_log("cat keys not retrieved")
        return False
    
    def get_machine_details(self):
        if self.kwargs['agent_settings'] is not None and 'deployment_config' in self.kwargs['agent_settings'] and self.kwargs['agent_settings']['deployment_config'] is not None:
            self.add_to_log("passed first test")
            deployment_config = self.kwargs['agent_settings']['deployment_config']
            if 'machine_serial_number' in deployment_config and 'machine_model' in deployment_config and 'machine_make' in deployment_config:
                self.machine_serial_number = deployment_config['machine_serial_number']
                self.machine_model = deployment_config['machine_model']
                self.machine_make = deployment_config['machine_make']
                self.add_to_log("machine details retrieved " + str(self.machine_serial_number) + " " + str(self.machine_model) + " " + str(self.machine_make))
                return True
        return False
            
    def add_to_log(self, msg):
        if not hasattr(self, '_log'):
            self._log = ""
        self._log = self._log + str(msg) + "\n"

    def get_sms_alert_days(self):
        cmds_obj = self.ui_cmds_channel.get_aggregate()
        try: return cmds_obj['cmds']['warningSmsPeriod']
        except: return 14

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