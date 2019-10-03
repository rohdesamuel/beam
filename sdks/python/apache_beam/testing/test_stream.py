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

"""Provides TestStream for verifying streaming runner semantics.

For internal use only; no backwards-compatibility guarantees.
"""
from __future__ import absolute_import

import grpc

from abc import ABCMeta
from abc import abstractmethod
from builtins import object
from functools import total_ordering

from future.utils import with_metaclass

from apache_beam import coders
from apache_beam import core
from apache_beam import pvalue
from apache_beam.portability.api import beam_interactive_api_pb2
from apache_beam.portability.api import beam_interactive_api_pb2_grpc
from apache_beam.transforms import PTransform
from apache_beam.transforms import window
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp
from apache_beam.utils.windowed_value import WindowedValue

__all__ = [
    'Event',
    'ElementEvent',
    'WatermarkEvent',
    'ProcessingTimeEvent',
    'TestStream',
    ]


@total_ordering
class Event(with_metaclass(ABCMeta, object)):
  """Test stream event to be emitted during execution of a TestStream."""

  @abstractmethod
  def __eq__(self, other):
    raise NotImplementedError

  @abstractmethod
  def __hash__(self):
    raise NotImplementedError

  @abstractmethod
  def __lt__(self, other):
    raise NotImplementedError

  def __ne__(self, other):
#TODO(BEAM - 5949) : Needed for Python 2 compatibility.
    return not self == other


class TaggedElement(object):
  def __init__(self, tag, timestamp, value):
    self.tag = tag
    self.timestamp = timestamp
    self.value = value

  def __eq__(self, other):
    return (self.tag == other.tag and
            self.timestamp == other.timestamp and
            self.value == other.value)

  def __lt__(self, other):
    return self.timestamp < other.timestamp


class TaggedElementEvent(Event):
  """Element-producing test stream event."""

  def __init__(self, elements):
    self.elements = elements

  def __eq__(self, other):
    return self.elements == other.elements

  def __hash__(self):
    return hash(self.timestamped_values)

  def __lt__(self, other):
    return self.timestamped_values < other.timestamped_values


class ElementEvent(Event):
  """Element-producing test stream event."""

  def __init__(self, timestamped_values, tag=None):
    self.timestamped_values = timestamped_values
    self.tag = tag

  def __eq__(self, other):
    return (self.timestamped_values == other.timestamped_values and
            self.tag == other.tag)

  def __hash__(self):
    return hash(self.timestamped_values)

  def __lt__(self, other):
    return self.timestamped_values < other.timestamped_values


class WatermarkEvent(Event):
  """Watermark-advancing test stream event."""

  def __init__(self, new_watermark):
    self.new_watermark = timestamp.Timestamp.of(new_watermark)

  def __eq__(self, other):
    return self.new_watermark == other.new_watermark

  def __hash__(self):
    return hash(self.new_watermark)

  def __lt__(self, other):
    return self.new_watermark < other.new_watermark


class ProcessingTimeEvent(Event):
  """Processing time-advancing test stream event."""

  def __init__(self, advance_by):
    self.advance_by = timestamp.Duration.of(advance_by)

  def __eq__(self, other):
    return self.advance_by == other.advance_by

  def __hash__(self):
    return hash(self.advance_by)

  def __lt__(self, other):
    return self.advance_by < other.advance_by

class _MultiTestStream(PTransform):
  def __init__(self, test_stream, tags, main_tag):
    super(_MultiTestStream, self).__init__(test_stream.label)
    self._test_stream = test_stream
    self._tags = tags
    self._main_tag = main_tag

  def expand(self, pcoll):
    _ = pcoll | self._test_stream
    return pvalue.DoOutputsTuple(
        pcoll.pipeline, self._test_stream, self._tags, self._main_tag)

class TestStream(PTransform):
  """Test stream that generates events on an unbounded PCollection of elements.

  Each event emits elements, advances the watermark or advances the processing
  time.  After all of the specified elements are emitted, ceases to produce
  output.
  """
  def __init__(self, coder=coders.FastPrimitivesCoder, endpoint=''):
    assert coder is not None
    self.coder = coder
    self.current_watermark = timestamp.MIN_TIMESTAMP
    self.output_tags = set()
    self._events = []
    self._endpoint = endpoint
    self._is_done = False

  def get_windowing(self, unused_inputs):
    return core.Windowing(window.GlobalWindows())

  def with_outputs(self, *tags, **main_kw):
    main_tag = main_kw.pop('main', None)
    if main_kw:
      raise ValueError('Unexpected keyword arguments: %s' %
                       list(main_kw))
    return _MultiTestStream(self, tags, main_tag)

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin)
    self.pipeline = pbegin.pipeline
    return pvalue.PCollection(self.pipeline, is_bounded=False)

  def _infer_output_coder(self, input_type=None, input_coder=None):
    return self.coder

  def _add(self, event):
    if isinstance(event, ElementEvent):
      for tv in event.timestamped_values:
        assert tv.timestamp < timestamp.MAX_TIMESTAMP, (
            'Element timestamp must be before timestamp.MAX_TIMESTAMP.')
    elif isinstance(event, WatermarkEvent):
      assert event.new_watermark > self.current_watermark, (
          'Watermark must strictly-monotonically advance.')
      self.current_watermark = event.new_watermark
    elif isinstance(event, ProcessingTimeEvent):
      assert event.advance_by > 0, (
          'Must advance processing time by positive amount.')
    else:
      raise ValueError('Unknown event: %s' % event)
    self._events.append(event)

  def has_events(self):
    return len(self._events) > 0

  def _events_from_rpc(self):
    channel = grpc.insecure_channel(self._endpoint)
    stub = beam_interactive_api_pb2_grpc.InteractiveServiceStub(channel)
    request = beam_interactive_api_pb2.EventsRequest()
    for response in stub.Events(request):
      if response.end_of_stream:
        self._is_done = True
      else:
        self._is_done = False
      for event in response.events:
        if event.HasField('watermark_event'):
          yield WatermarkEvent(event.watermark_event.new_watermark)
        elif event.HasField('processing_time_event'):
          yield ProcessingTimeEvent(
              event.processing_time_event.advance_duration)
        elif event.HasField('element_event'):
          for element in event.element_event.elements:
            value = self.coder().decode(element.encoded_element)
            yield ElementEvent([TimestampedValue(value, element.timestamp)])

  def _events_from_script(self, index):
    if len(self._events) == 0:
      return
    yield self._events[index]

  def events(self, index):
    if self._endpoint:
      return self._events_from_service()
    else:
      return self._events_from_script(index)

  def begin(self):
    return 0

  def end(self, index):
    if self._endpoint:
      return self._is_done
    return index == len(self._events)

  def next(self, index):
    if not self._endpoint:
      return index + 1
    return 0

  def add_elements(self, elements, tag=None):
    """Add elements to the TestStream.

    Elements added to the TestStream will be produced during pipeline execution.
    These elements can be TimestampedValue, WindowedValue or raw unwrapped
    elements that are serializable using the TestStream's specified Coder.  When
    a TimestampedValue or a WindowedValue element is used, the timestamp of the
    TimestampedValue or WindowedValue will be the timestamp of the produced
    element; otherwise, the current watermark timestamp will be used for that
    element.  The windows of a given WindowedValue are ignored by the
    TestStream.
    """
    timestamped_values = []
    for element in elements:
      if isinstance(element, TimestampedValue):
        timestamped_values.append(element)
      elif isinstance(element, WindowedValue):
        # Drop windows for elements in test stream.
        timestamped_values.append(
            TimestampedValue(element.value, element.timestamp))
      else:
        # Add elements with timestamp equal to current watermark.
        timestamped_values.append(
            TimestampedValue(element, self.current_watermark))
    self._add(ElementEvent(timestamped_values, tag))
    return self

  def advance_watermark_to(self, new_watermark):
    """Advance the watermark to a given Unix timestamp.

    The Unix timestamp value used must be later than the previous watermark
    value and should be given as an int, float or utils.timestamp.Timestamp
    object.
    """
    self._add(WatermarkEvent(new_watermark))
    return self

  def advance_watermark_to_infinity(self):
    """Advance the watermark to the end of time, completing this TestStream."""
    self.advance_watermark_to(timestamp.MAX_TIMESTAMP)
    return self

  def advance_processing_time(self, advance_by):
    """Advance the current processing time by a given duration in seconds.

    The duration must be a positive second duration and should be given as an
    int, float or utils.timestamp.Duration object.
    """
    self._add(ProcessingTimeEvent(advance_by))
    return self
