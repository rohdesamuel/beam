#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import grpc
import time

from apache_beam.portability.api import beam_interactive_api_pb2
from apache_beam.portability.api import beam_interactive_api_pb2_grpc
from apache_beam.portability.api.beam_interactive_api_pb2_grpc import InteractiveServiceServicer
from concurrent.futures import ThreadPoolExecutor


def to_api_state(state):
  if state == 'STOPPED':
    return beam_interactive_api_pb2.StatusResponse.STOPPED
  if state == 'PAUSED':
    return beam_interactive_api_pb2.StatusResponse.PAUSED
  return beam_interactive_api_pb2.StatusResponse.RUNNING


from http.server import HTTPServer, BaseHTTPRequestHandler
import ssl


class ForwardingServer:
  class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
      self.send_response(200)
      self.send_header('Content-type', 'text/html')
      self.send_header('Access-Control-Allow-Origin', '*')
      self.end_headers()
      self.wfile.write('OK'.encode('utf8'))

      import grpc
      from apache_beam.portability.api import beam_interactive_api_pb2
      from apache_beam.portability.api import beam_interactive_api_pb2_grpc

      channel = grpc.insecure_channel('localhost:12345')
      stub = beam_interactive_api_pb2_grpc.InteractiveServiceStub(channel)
      stub.Stop(beam_interactive_api_pb2.StopRequest())

    def log_message(self, format, *args):
      return


  def __init__(self, port=12346):
    self._port = port
    self._httpd = HTTPServer(('localhost', self._port), ForwardingServer.Handler)

  def start(self):
    import threading
    def run():
      self._httpd.serve_forever()
    self._t = threading.Thread(target=run)
    self._t.start()

  def stop(self):
    self._httpd.shutdown()
    self._httpd.server_close()
    self._t.join()


class InteractiveStreamController(InteractiveServiceServicer):
  def __init__(self, endpoint, streaming_cache):
    self._endpoint = endpoint
    self._streaming_cache = streaming_cache
    self._state = 'STOPPED'
    self._playback_speed = 1.0
    self.result = None

  def start(self, result):
    self.result = result

    self._server = grpc.server(ThreadPoolExecutor(max_workers=10))
    beam_interactive_api_pb2_grpc.add_InteractiveServiceServicer_to_server(
        self, self._server)
    self._server.add_insecure_port(self._endpoint)
    self._forwarding_server = ForwardingServer()

    self._server.start()
    self._forwarding_server.start()

  def stop(self):
    self._forwarding_server.stop()
    self._server.stop(0.5)
    self._server = None
    self._forwarding_server = None

  def Start(self, request, context):
    """Requests that the Service starts emitting elements.
    """

    self._next_state('RUNNING')
    self._playback_speed = request.playback_speed or 1.0
    self._playback_speed = max(min(self._playback_speed, 1000000.0), 0.001)
    return beam_interactive_api_pb2.StartResponse()

  def Stop(self, request, context):
    """Requests that the Service stop emitting elements.
    """
    if self.result:
      print('stopping job')
      self.result.cancel()
      self._next_state('STOPPED')
      self.result = None
    return beam_interactive_api_pb2.StopResponse()

  def Pause(self, request, context):
    """Requests that the Service pause emitting elements.
    """
    self._next_state('PAUSED')
    return beam_interactive_api_pb2.PauseResponse()

  def Step(self, request, context):
    """Requests that the Service emit a single element from each cached source.
    """
    self._next_state('STEP')
    return beam_interactive_api_pb2.StepResponse()

  def Status(self, request, context):
    """Returns the status of the service.
    """
    resp = beam_interactive_api_pb2.StatusResponse()
    resp.stream_time.GetCurrentTime()
    resp.state = to_api_state(self._state)
    return resp

  def _reset_state(self):
    self._reader = None
    self._playback_speed = 1.0
    self._state = 'STOPPED'

  def _next_state(self, state):
    if not self._state or self._state == 'STOPPED':
      if state == 'RUNNING' or state == 'STEP':
        self._reader = self._streaming_cache.reader()
    elif self._state == 'RUNNING':
      if state == 'STOPPED':
        self._reset_state()
    self._state = state

  def Events(self, request, context):
    # The TestStream will wait until the stream starts.
    while self._state != 'RUNNING' and self._state != 'STEP':
      time.sleep(0.01)

    events = self._reader.read()
    if events:
      for e in events:
        # Here we assume that the first event is the processing_time_event so
        # that we can sleep and then emit the element. Thereby, trying to
        # emulate the original stream.
        if e.HasField('processing_time_event'):
          sleep_duration = (
              e.processing_time_event.advance_duration / self._playback_speed
              ) * 10**-6
          time.sleep(sleep_duration)
        yield beam_interactive_api_pb2.EventsResponse(events=[e])
    else:
      resp = beam_interactive_api_pb2.EventsResponse()
      resp.end_of_stream = True
      self._next_state('STOPPED')
      yield resp
      return

    # The Step command allows the user to send an individual element from each
    # source down into the pipeline. It immediately pauses afterwards.
    if self._state == 'STEP':
      self._next_state('PAUSED')
