from .context_base import AcquisitionOptimizer


class RandomSearch(AcquisitionOptimizer):
    def __init__(self, acquisition_function, config_space, rng=None, context=None, context_flag=True):
        super().__init__(acquisition_function, config_space, rng, context=context, context_flag=context_flag)

    def _maximize(self, observations, num_points, _sorted=False, **kwargs):
        if num_points > 1:
            rand_configs = self.config_space.sample_configuration(
                size=num_points)
        else:
            rand_configs = [self.config_space.sample_configuration(size=1)]

        if _sorted:
            for i in range(len(rand_configs)):
                rand_configs[i].origin = 'Random Search (sorted)'
            return self._sort_configs_by_acq_value(rand_configs)
        else:
            for i in range(len(rand_configs)):
                rand_configs[i].origin = 'Random Search'
            return [(0, rand_configs[i]) for i in range(len(rand_configs))]
