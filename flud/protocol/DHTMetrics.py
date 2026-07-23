import time


class DHTOperationMetrics:
    def __init__(self, collector, op_type, **context):
        self.collector = collector
        self.op_type = op_type
        self.context = context
        self.started_at = time.time()
        self._started_perf = time.perf_counter()
        self.completed_at = None
        self.duration_seconds = None
        self.status = "in_progress"
        self.error = None
        self.timed_out = False
        self.round_count = 0
        self.rpc_attempts = 0
        self.rpc_failures = 0
        self.rpc_timeouts = 0
        self._queried_nodes = set()
        self.returned_candidate_count = 0
        self.value_found = False
        self.value_response_count = 0
        self.early_terminated = False
        self.store_destination_count = 0
        self.store_success_count = 0
        self.store_failure_count = 0
        self.store_timeout_count = 0
        self.alpha_samples = []
        self.cache_hit = False
        self.write_quorum = None

    @property
    def queried_node_count(self):
        return len(self._queried_nodes)

    def record_round(self):
        self.round_count += 1

    def record_rpc_attempt(self):
        self.rpc_attempts += 1

    def record_rpc_failure(self, timed_out=False):
        self.rpc_failures += 1
        if timed_out:
            self.rpc_timeouts += 1

    def record_queried_node(self, node_id):
        if node_id is not None:
            self._queried_nodes.add(int(node_id))

    def set_returned_candidate_count(self, count):
        self.returned_candidate_count = int(count)

    def set_value_found(self, found):
        self.value_found = bool(found)

    def record_value_response(self):
        self.value_response_count += 1

    def set_early_terminated(self, terminated):
        self.early_terminated = bool(terminated)

    def set_store_destination_count(self, count):
        self.store_destination_count = int(count)

    def record_alpha(self, alpha):
        self.alpha_samples.append(int(alpha))

    def set_cache_hit(self, hit):
        self.cache_hit = bool(hit)

    def set_write_quorum(self, quorum):
        self.write_quorum = int(quorum)

    def record_store_success(self):
        self.store_success_count += 1

    def record_store_failure(self, timed_out=False):
        self.store_failure_count += 1
        if timed_out:
            self.store_timeout_count += 1

    def finish_success(self):
        self.status = "success"
        self._finish()
        return self

    def finish_failure(self, error, timed_out=False):
        self.status = "failure"
        self.error = {
            "type": error.__class__.__name__,
            "message": str(error),
        }
        self.timed_out = bool(timed_out)
        self._finish()
        return self

    def _finish(self):
        if self.completed_at is None:
            self.completed_at = time.time()
            self.duration_seconds = time.perf_counter() - self._started_perf
            self.collector.operations.append(self)

    def to_dict(self):
        data = {
            "op_type": self.op_type,
            "context": self.context,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "timed_out": self.timed_out,
            "round_count": self.round_count,
            "rpc_attempts": self.rpc_attempts,
            "rpc_failures": self.rpc_failures,
            "rpc_timeouts": self.rpc_timeouts,
            "queried_node_count": self.queried_node_count,
            "returned_candidate_count": self.returned_candidate_count,
            "value_found": self.value_found,
            "value_response_count": self.value_response_count,
            "early_terminated": self.early_terminated,
            "store_destination_count": self.store_destination_count,
            "store_success_count": self.store_success_count,
            "store_failure_count": self.store_failure_count,
            "store_timeout_count": self.store_timeout_count,
            "alpha_samples": self.alpha_samples,
            "cache_hit": self.cache_hit,
            "write_quorum": self.write_quorum,
        }
        if self.error is not None:
            data["error"] = self.error
        return data


class DHTMetricsCollector:
    def __init__(self):
        self.operations = []

    def start_operation(self, op_type, **context):
        return DHTOperationMetrics(self, op_type, **context)

    def to_dict(self):
        return [operation.to_dict() for operation in self.operations]
