#!/usr/bin/python3

import os, json, requests, time, base64, shutil


class doover_api_iface:

    def __init__(
            self,
            agent_id=None,
            access_token=None,
            endpoint="https://my.doover.dev",
            debug_mode=False,
            verify=True,
        ):

        self.agent_id = agent_id
        self.access_token = access_token
        
        self.endpoint = endpoint

        self.debug_mode = debug_mode
        self.verify = verify

    def set_access_token(self, access_token):
        self.access_token = access_token

    def get_headers(self):
        return {"Authorization": "Token " + str(self.access_token)}

    def make_get_request(self, url, data=None):
        full_url = self.endpoint + url
        r = requests.get(full_url, data=data, headers=self.get_headers(), verify=self.verify)
        if r.status_code == 200:
            if self.debug_mode:
                print(r.text)
            return r
        else:
            print("ERROR : " + str(r.status_code))
            print(r.text)
            return None


    def make_post_request(self, url, data=None):
        full_url = self.endpoint + url
        r = requests.post(full_url, data=data, headers=self.get_headers(), verify=self.verify)
        if r.status_code == 200:
            if self.debug_mode:
                print(r.text)
            return r
        else:
            print("ERROR : " + str(r.status_code))
            print(r.text)
            return None


    def get_agent_details(self, agent_id):

        url = "/ch/v1/agent/" + str(agent_id) + "/"
        res = self.make_get_request(
            url=url,
            data=None,
        )

        return json.loads( res.text )

    
    def get_channel_details(self, channel_id=None, agent_id=None, channel_name=None):

        if channel_id is not None:
            url = '/ch/v1/channel/' + str(channel_id) + '/'
            msgs_url = '/ch/v1/channel/' + str(channel_id) + '/messages/'
        elif agent_id is not None and channel_name is not None:
            url = "/ch/v1/agent/" + str(agent_id) + "/" + str(channel_name) + "/"
            msgs_url = "/ch/v1/agent/" + str(agent_id) + "/" + str(channel_name) + "/messages/"
        else:
            args = {
                "channel_id" : channel_id,
                "agent_id" : agent_id,
                "channel_name" : channel_name,
            }
            raise Exception("Incorrect arguments supplied to get_channel_details : " + str(args))

        res = json.loads( 
            self.make_get_request(
                url=url,
                data=None,
            ).text
        )

        msgs_res = json.loads(
            self.make_get_request(
                url=msgs_url,
                data=None,
            ).text
        )

        res['messages'] = msgs_res['messages']

        return res

    
    def get_message_details(self, channel_id, message_id):

        url = '/ch/v1/channel/' + str(channel_id) + '/message/' + str(message_id) #+ "/"
        res = self.make_get_request(
            url=url,
            data=None,
        )

        return json.loads( res.text )


    def publish_to_channel(self, msg_str, channel_id=None, agent_id=None, channel_name=None):

        if channel_id is not None:
            url = '/ch/v1/channel/' + str(channel_id) + '/'
        elif agent_id is not None and channel_name is not None:
            url = "/ch/v1/agent/" + str(agent_id) + "/" + str(channel_name) + "/"
        else:
            args = {
                "channel_id" : channel_id,
                "agent_id" : agent_id,
                "channel_name" : channel_name,
            }
            raise Exception("Incorrect arguments supplied to publish_to_channel : " + str(args))

          
        res = self.make_post_request(
            url=url,
            data=msg_str,
        ).text

        output = {
            'msg_id' : res
        }

        return output


class message_log:

    def __init__(
            self,
            api_client,
            channel_id=None,
            message_id=None,
        ):

        self.api_client = api_client

        self.channel_id = channel_id
        self.message_id = message_id
        self.json_result = None


    def update(self):

        result = self.api_client.get_message_details(
            channel_id=self.channel_id,
            message_id=self.message_id,
        )
        
        self.json_result = result

        return result

    def get_payload(self):
        if self.json_result is None:
            self.update()

        return self.json_result['payload']


class channel:

    def __init__(
            self,
            api_client,
            channel_id=None,
            agent_id=None,
            channel_name=None,
        ):

        self.api_client = api_client

        self.channel_id = channel_id

        self.agent_id = agent_id
        self.channel_name = channel_name

        self.json_result = None


    def update(self):

        result = self.api_client.get_channel_details(
            channel_id=self.channel_id,
            agent_id=self.agent_id,
            channel_name=self.channel_name,
        )

        self.json_result = result

        self.channel_id = result['channel']
        self.agent_id = result['owner']
        self.channel_name = result['name']


    def get_aggregate(self):

        if self.json_result is None:
            self.update()

        return self.json_result['aggregate']['payload']


    def get_messages(self):

        if self.json_result is None:
            self.update()

        messages = self.json_result['messages']
        result = []
        for m in messages:

            message_id = m['message']
            agent_id = m['agent']
            channel_id = self.channel_id

            new_message = message_log(
                api_client=self.api_client,
                channel_id=channel_id,
                message_id=message_id,
            )

            result.append(new_message)

        return result
 

    def publish(self, msg_str, save_log=True, log_aggregate=False ):

        result = self.api_client.publish_to_channel(
            msg_str=msg_str,
            channel_id=self.channel_id,
            agent_id=self.agent_id,
            channel_name=self.channel_name,
        )

        return result


class agent:

    def __init__(
            self,
            agent_id,
            api_client,
        ):

        self.agent_id = agent_id
        self.api_client = api_client

        self.json_result = None


    def update(self):

        result = self.api_client.get_agent_details(self.agent_id)
        self.json_result = result


    def get_channels(self):

        if self.json_result is None:
            self.update()

        channels = self.json_result['channels']
        result = {}
        for c in channels:

            channel_id = c['channel']
            agent_id = c['agent']
            channel_name = c['name']

            new_channel = channel(
                api_client=self.api_client,
                channel_id=channel_id,
                agent_id=agent_id,
                channel_name=channel_name,
            )

            result[channel_name] = new_channel

        return result



class doover_iface:

    def __init__(
            self,
            agent_id=None,
            access_token=None,
            endpoint="https://my.doover.dev",
            debug_mode=False,
            verify_ssl=True,
        ):

        self.agent_id = agent_id
        self.access_token = access_token
        self.access_token_expiry = None

        self.endpoint = endpoint
        self.debug_mode = debug_mode
        self.verify_ssl = verify_ssl

        self.api_client = doover_api_iface(
            agent_id=agent_id,
            access_token=access_token,
            endpoint=endpoint,
            debug_mode=debug_mode,
            verify=verify_ssl,
        )

    def get_agent(self, agent_id):

        return agent(
            agent_id=agent_id,
            api_client=self.api_client
        )

    def get_channel(self, channel_id=None, channel_name=None, agent_id=None):

        return channel(
            channel_id=channel_id,
            channel_name=channel_name,
            agent_id=agent_id,
            api_client=self.api_client
        )

