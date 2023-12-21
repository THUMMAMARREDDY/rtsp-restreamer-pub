import sys
import gi
import time
import logging
from functools import partial
from datetime import datetime

import os
# os.environ["GST_DEBUG"] = "3"
os.environ["GST_DEBUG"] = "0"

# Set the version before import to avoid warning
gi.require_version("Gst", "1.0")
gi.require_version("GstRtspServer", "1.0")
from gi.repository import Gst, GstRtspServer, GLib

ACTIVE_STREAMS = {}
RESTREAMERS = {}

# Configure logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class StreamRestreamer:
    def __init__(self, server, input_url, endpoint):
        self.server = server
        self.input_url = input_url
        self.endpoint = endpoint 
        self.endpoint_str = self.endpoint + '/stream=0'
        self.media = None
        RESTREAMERS[self.endpoint_str] = self
        self.streaming_started_time = None
        self.streaming_ended_time = None

    def create_pipeline_str(self):
        pipeline_str = (
            f"rtspsrc location={self.input_url} protocols=tcp latency=0 name=src ! "
            f"rtpjitterbuffer drop-on-latency=true latency=0 ! "
            f"rtph264depay ! "
            # f"identity silent=false signal-handoffs=true name=watcher ! "
            f"queue max-size-buffers=2000 max-size-time=0 max-size-bytes=0 ! "
            f"rtph264pay name=pay0 pt=96"
        )
        logger.debug(f"Pipeline string for {self.input_url}: {pipeline_str}")
        return pipeline_str
    
    def remove_media(self):
        # logger.info(f"Removing media for {self.endpoint_str}")
        # print("Removing media Now!!")
        if self.media:
            pipeline = self.media.get_element()
            pipeline.set_state(Gst.State.NULL)
            self.media.unprepare()
        self.server.get_mount_points().remove_factory(self.endpoint)
        self.media = None
        GLib.timeout_add_seconds(1, self.restream)

    def restream(self):
        # logger.info(f"Restreaming {self.input_url} at rtsp://localhost:8554{self.endpoint}")
        factory = GstRtspServer.RTSPMediaFactory()
        factory.set_launch(self.create_pipeline_str())
        factory.set_shared(True)
        factory.connect('media-configure', self.on_media_configure)
        factory.connect('media-constructed', self.on_media_constructed)
        factory.set_eos_shutdown(True)
        self.server.get_mount_points().add_factory(self.endpoint, factory)

    def on_media_constructed(self, factory, media):
        # logger.info(f"Media constructed for {self.endpoint_str}")
        # print("On Media Constructed")
        pass

    def on_media_configure(self, factory, media):
        # logger.info(f"Media configured for {self.endpoint_str}")
        # print("On Media Configure")
        # Check how many clients are connected
        if self.endpoint_str in ACTIVE_STREAMS:
            num_clients = ACTIVE_STREAMS[self.endpoint_str]
            # logger.info(f"Number of clients for {self.endpoint_str}: {num_clients}")
            # print(f"Number of clients for {self.endpoint_str}: {num_clients}")
            if num_clients == 0:
                self.remove_media()

        self.media = media
        pipeline = media.get_element()

        rtspsrc = pipeline.get_by_name("src")
        rtspsrc.connect("pad-added", self.on_pad_added)
        rtspsrc.connect("pad-removed", self.on_pad_removed)

    def on_pad_added(self, src, pad):
        # logger.info(f"New pad added from {src.get_name()}. Stream is likely up. {self.endpoint_str})")

        if self.streaming_started_time is None:
            self.streaming_started_time = datetime.now()
            # logger.info(f"Streaming started for {self.endpoint_str}")
            return
        
        time_diff = datetime.now() - self.streaming_started_time
        self.streaming_started_time = datetime.now()
        if time_diff.total_seconds() < 2:
            return
        
        if self.streaming_ended_time is None:
            return
        
        offline_duration = (self.streaming_started_time - self.streaming_ended_time).total_seconds()
        logger.info(f"Time to reconnect for {self.endpoint_str}: {offline_duration} seconds")
        
    def on_pad_removed(self, src, pad):
        # logger.info(f"Pad removed from {src.get_name()}. Stream is likely down. {self.endpoint_str})")

        if self.streaming_ended_time is not None:
            time_diff = datetime.now() - self.streaming_ended_time
            if time_diff.total_seconds() < 1:
                return
        
        self.streaming_ended_time = datetime.now()
        streaming_duration = (self.streaming_ended_time - self.streaming_started_time).total_seconds()
        logger.info(f"Streaming ended for {self.endpoint_str}. Duration was: {streaming_duration} seconds")

    
def on_client_connected(server, client):
    # logger.info("Client connected")
    # Connect to the setup-request signal
    client.connect("setup-request", on_setup_request)
    # client.connect("teardown-request", on_teardown_request)
    client.connect("closed", partial(on_client_closed, server))
    # client.connect("closed", on_client_closed)

def on_client_closed(server, client):
    # logger.info("Client closed")
    # Get client IP
    # print("on_client_closed")
    media_uri = getattr(client, "media_uri", None)
    # print(f"media_uri: {media_uri}")
    connection = client.get_connection()
    client_address = ""
    if connection:
        remote_address = connection.get_ip()
        if remote_address:
            client_address = remote_address
    if media_uri:
        if media_uri in ACTIVE_STREAMS:
            # print(f"Decrementing active streams for {media_uri}")
            # print(f"Active Streams before: {ACTIVE_STREAMS}")
            ACTIVE_STREAMS[media_uri] -= 1
            # print(f"Active Streams after: {ACTIVE_STREAMS}")
            if ACTIVE_STREAMS[media_uri] == 0:
                del ACTIVE_STREAMS[media_uri]
                # Gracefully shut down the pipeline
                restreamer = RESTREAMERS.get(media_uri)
                if restreamer and restreamer.media:
                    # print("Shutting down pipeline")
                    restreamer.remove_media()
        
        logger.info(f"Client {client_address} closed connection: {media_uri}")
        logger.info(f"Active Streams: {ACTIVE_STREAMS}")

def on_setup_request(client, context):
    # logger.info("Client setup request")
    media_uri = context.uri.abspath
    # Get client IP
    connection = client.get_connection()
    client_address = ""
    if connection:
        remote_address = connection.get_ip()
        if remote_address:
            client_address = remote_address
    if media_uri:
        client.media_uri = media_uri
        if media_uri not in ACTIVE_STREAMS:
            ACTIVE_STREAMS[media_uri] = 0
        ACTIVE_STREAMS[media_uri] += 1
        logger.info(f"Client {client_address} is setting up stream: {media_uri}")
        logger.info(f"Active Streams: {ACTIVE_STREAMS}")


def main(args):
    config_file = 'config.txt'
    # config_file = 'u3bp_config.txt'

    with open(config_file, "r") as f:
        stream_configs = [line.strip().split() for line in f.readlines()]

    # Initialize GStreamer with debug logging
    Gst.init(["--gst-debug-level=0", "--clock-jitter=0"])

    server = GstRtspServer.RTSPServer.new()
    server.set_address("0.0.0.0")
    server.set_service("8554")

    for input_url, endpoint in stream_configs:
        restreamer = StreamRestreamer(server, input_url, endpoint)
        restreamer.restream()
        logger.info(
            f"Restreaming {input_url} at rtsp://localhost:8554{endpoint}")

    server.attach(None)
    server.connect("client-connected", on_client_connected)

    logger.info("RTSP server running...")

    GLib.MainLoop().run()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
