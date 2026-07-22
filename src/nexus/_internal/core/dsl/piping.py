from dataclasses import dataclass

from .flow import Flow
from .nodes import Pipes, Sinks, Sources


@dataclass
class Piping:
    """
    Aggregate flows into one validated set of source connections.

    Primary and tap declarations for repeated sources are merged through
    ``Pipes``; conflicting primary or overlapping role declarations fail.
    """

    pipes: Pipes
    sources: Sources
    sinks: Sinks

    def __init__(self):
        self.pipes = Pipes()
        self.sources = set()
        self.sinks = set()

    def add_flow(self, flow: Flow) -> None:
        self.pipes.merge(flow.pipes)
        self.sources.update(flow.sources)
        self.sinks.update(flow.sinks)
