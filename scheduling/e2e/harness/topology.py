"""Read-only test capacity profiles; operational queue manifests belong to Gitops."""
import argparse
from dataclasses import asdict, dataclass
import json


@dataclass(frozen=True)
class Topology:
    workers: tuple[str, ...]
    heavy: int
    light: int
    recovery: int = 4
    training_per_worker: int = 4
    spare_per_worker: int = 1

    @property
    def training(self):
        return len(self.workers) * self.training_per_worker

    def load_jobs(self):
        # One 4-GPU recovery subject; Light fills every other worker through borrowing.
        jobs = [('train-a', 4, 'queue-heavy', self.workers[0])]
        for i, worker in enumerate(self.workers[1:], 1):
            jobs.extend((f'lite-{i}-{j}', 2, 'queue-light', worker) for j in (1, 2))
        return jobs


PROFILES = {
    'gpu10': Topology(('worker1', 'worker2'), heavy=6, light=2),
    'gpu10-5-3': Topology(('worker1', 'worker2'), heavy=5, light=3),
    'full': Topology(('worker1', 'worker2', 'worker3'), heavy=8, light=4),
}


def profile(name):
    try:
        return PROFILES[name]
    except KeyError:
        raise ValueError(f'unknown E2E_PROFILE {name!r}; choose {", ".join(PROFILES)}') from None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', choices=PROFILES, default='gpu10')
    args = parser.parse_args()
    print(json.dumps(asdict(profile(args.profile)), indent=2))
