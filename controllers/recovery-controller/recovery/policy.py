"""Select a recovery action for a fault event."""


def plan(event):
    required = ('event_id', 'cluster')
    if not isinstance(event, dict) or any(not isinstance(event.get(k), str) or not event[k].strip() for k in required):
        raise ValueError('event_id and cluster must be nonempty strings')
    if event.get('job_id') is not None and not isinstance(event['job_id'], str):
        raise ValueError('job_id must be a string or null')
    from recovery.init_failure import decision
    init = decision(event)
    if not init and not event.get('suggested_action') and not event.get('fault_type'):
        raise ValueError('suggested_action or legacy fault_type required')
    if event['cluster'] not in ('eks-a', 'eks-b', 'eks-c'):
        raise ValueError('unknown cluster')
    actions = {
        'restart_in_place': 'RETRY_GROUP',
        'isolate_migrate': 'QUARANTINE_AND_RETRY',
        'reset_then_migrate': 'QUARANTINE_AND_RETRY',
        # Pre-save migration is deferred; use the normal generation-first spare path.
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
    if event.get('collector_healthy') is False and event.get('category') != 'site':
        action = 'HOLD'
    return {'event_id': event['event_id'], 'job_id': event.get('job_id'),
            'cluster': event['cluster'], 'suggested_action': action,
            'mode': 'observe', 'executed': False}
