from __future__ import annotations

from ansq.typedefs import HTTPResponse

from .base import NSQHTTPConnection


class NsqLookupd(NSQHTTPConnection):
    """HTTP client for nsqlookupd — the service-discovery daemon.

    :see: http://nsq.io/components/nsqlookupd.html
    """

    async def ping(self) -> HTTPResponse:
        """Monitoring endpoint.

        :returns: ``"OK"`` if the server is healthy, otherwise raises an exception.
        """
        return await self.perform_request("GET", "ping", None, None)

    async def info(self) -> HTTPResponse:
        """Return version information about the nsqlookupd instance."""
        response = await self.perform_request("GET", "info", None, None)
        return response

    async def lookup(self, topic: str) -> HTTPResponse:
        """Return producer information for all nsqd nodes publishing a given topic.

        :param topic: The topic name to look up.
        :returns: A dict with a ``producers`` key listing nodes that have the topic.
        """
        response = await self.perform_request("GET", "lookup", {"topic": topic}, None)
        return response

    async def topics(self) -> HTTPResponse:
        """Return a list of all known topics registered with this nsqlookupd."""
        resp = await self.perform_request("GET", "topics", None, None)
        return resp

    async def channels(self, topic: str) -> HTTPResponse:
        """Return a list of all known channels for a given topic.

        :param topic: The topic name to look up channels for.
        """
        resp = await self.perform_request("GET", "channels", {"topic": topic}, None)
        return resp

    async def nodes(self) -> HTTPResponse:
        """Return a list of all known nsqd nodes registered with this nsqlookupd."""
        resp = await self.perform_request("GET", "nodes", None, None)
        return resp

    async def create_topic(self, topic: str) -> HTTPResponse:
        """Create a topic on the nsqlookupd.

        :param topic: The topic name to create.
        """
        resp = await self.perform_request(
            "POST", "/topic/create", {"topic": topic}, None
        )
        return resp

    async def delete_topic(self, topic: str) -> HTTPResponse:
        """Delete a topic and all its channels from the nsqlookupd.

        :param topic: The topic name to delete.
        """
        resp = await self.perform_request(
            "POST", "/topic/delete", {"topic": topic}, None
        )
        return resp

    async def create_channel(self, topic: str, channel: str) -> HTTPResponse:
        """Create a channel for a given topic on the nsqlookupd.

        :param topic: The topic name.
        :param channel: The channel name to create.
        """
        resp = await self.perform_request(
            "POST", "/channel/create", {"topic": topic, "channel": channel}, None
        )
        return resp

    async def delete_channel(self, topic: str, channel: str) -> HTTPResponse:
        """Delete a channel from a given topic on the nsqlookupd.

        :param topic: The topic name.
        :param channel: The channel name to delete.
        """
        resp = await self.perform_request(
            "POST", "/channel/delete", {"topic": topic, "channel": channel}, None
        )
        return resp

    async def tombstone_topic_producer(self, topic: str, node: str) -> HTTPResponse:
        """Tombstone a specific producer for an existing topic.

        A tombstoned producer will be removed from lookup results for a brief
        period, allowing a rolling restart of nsqd nodes without consumers
        temporarily connecting to a node that is restarting.

        :param topic: The topic name.
        :param node: The producer node in ``<broadcast_address>:<http_port>`` format.
        :see: https://nsq.io/components/nsqlookupd.html
        """
        resp = await self.perform_request(
            "POST", "/topic/tombstone", {"topic": topic, "node": node}, None
        )
        return resp
