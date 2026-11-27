"""Select recovery by failure domain, not by the mere presence of a node name."""

GPU_SOURCES = {'xid', 'gpu', 'dcgm', 'dmesg', 'driver'}
MIGRATION_RECOMMENDATIONS = {'isolate_migrate', 'reset_then_migrate', 'evacuate_preemptive'}
# These hardware/driver faults cannot be repaired by replacing a Kubernetes Pod.
GPU_MIGRATION_XIDS = {48, 63, 64, 79, 119, 120}


def is_straggler(event):
    return event.get('category') == 'straggler' or event.get('fault_type') == 'straggler'


def gpu_migration_fault(event):
    raw = event.get('raw')
    xid = raw.get('xid') if isinstance(raw, dict) else None
    correlated = event.get('correlated')
    gpu_correlated = (isinstance(correlated, list) and any(
        isinstance(item, dict) and item.get('source') in GPU_SOURCES for item in correlated))
    return ((type(xid) is int and xid in GPU_MIGRATION_XIDS)
            or (event.get('source') in GPU_SOURCES
                and (event.get('category') in ('hw', 'gpu', 'driver')
                     or event.get('suggested_action') in MIGRATION_RECOMMENDATIONS))
            or (gpu_correlated and event.get('category') in ('hw', 'gpu', 'driver')
                and event.get('suggested_action') in MIGRATION_RECOMMENDATIONS))


def native_node_restart(event):
    """Kubernetes node loss: group restart without pinning to the unavailable old nodes."""
    return (event.get('source') == 'node' and not gpu_migration_fault(event)
            and event.get('fault_type') != 'spot_interruption')


def plan(event):
    required = ('event_id', 'cluster')
    if not isinstance(event, dict) or any(not isinstance(event.get(k), str) or not event[k].strip() for k in required):
        raise ValueError('event_id and cluster must be nonempty strings')
    if event.get('job_id') is not None and not isinstance(event['job_id'], str):
        raise ValueError('job_id must be a string or null')
    if event['cluster'] not in ('eks-a', 'eks-b', 'eks-c'):
        return {'event_id': event['event_id'], 'job_id': event.get('job_id'),
                'cluster': event['cluster'], 'suggested_action': 'HOLD',
                'reason': 'unknown_cluster', 'mode': 'observe', 'executed': False}
    from recovery.init_failure import decision
    init = decision(event)
    if not init and not event.get('suggested_action') and not event.get('fault_type'):
        raise ValueError('suggested_action or legacy fault_type required')
    actions = {
        'restart_in_place': 'RETRY_GROUP',
        'isolate_migrate': 'QUARANTINE_AND_RETRY',
        'reset_then_migrate': 'QUARANTINE_AND_RETRY',
        'evacuate_preemptive': 'QUARANTINE_AND_RETRY',
        'fence_evacuate': 'FENCE_AND_EVACUATE',
        'hold': 'HOLD', 'refuse': 'NEEDS_CONFIG_FIX', 'none': 'NONE',
    }
    legacy = {'software_transient': 'restart_in_place',
              'software_permanent': 'refuse', 'node_hardware': 'isolate_migrate',
              'spot_interruption': 'evacuate_preemptive', 'straggler': 'hold',
              'az_failure': 'fence_evacuate'}
    recommendation = event.get('suggested_action', legacy.get(event.get('fault_type'), 'hold'))
    if not init and recommendation not in actions:
        raise ValueError('unknown suggested_action')
    action = init['action'] if init else actions[recommendation]
    reason = None
    if not init and action in ('RETRY_GROUP', 'QUARANTINE_AND_RETRY'):
        if event.get('source') == 'spot' or event.get('fault_type') == 'spot_interruption':
            action, reason = 'QUARANTINE_AND_RETRY', 'spot_node_will_be_removed'
        elif gpu_migration_fault(event):
            action, reason = 'QUARANTINE_AND_RETRY', 'gpu_or_driver_migration'
        elif native_node_restart(event):
            node = event.get('node')
            action = 'RETRY_GROUP' if isinstance(node, str) and node.strip() else 'HOLD'
            reason = 'native_node_restart_unpinned' if action == 'RETRY_GROUP' else 'node_identity_unresolved'
        elif event.get('source') == 'pod':
            action, reason = 'RETRY_GROUP', 'pod_group_restart'
    if is_straggler(event):
        action, reason = 'HOLD', 'straggler_policy_required'
    if event.get('collector_healthy') is False and event.get('category') != 'site':
        action, reason = 'HOLD', 'collector_unhealthy'
    result = {'event_id': event['event_id'], 'job_id': event.get('job_id'),
              'cluster': event['cluster'], 'suggested_action': action,
              'mode': 'observe', 'executed': False}
    if reason:
        result['reason'] = reason
    return result
