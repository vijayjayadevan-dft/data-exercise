from settings.config import Local, Production, Staging
import os

config_space = os.getenv('CONFIG_PROFILE', None)
if config_space:
    if config_space == 'LOCAL':
        auto_config = Local
    elif config_space == 'STAGING':
        auto_config = Staging
    elif config_space == 'PRODUCTION':
        auto_config = Production
    else:
        auto_config = None
        raise EnvironmentError(f'CONFIG_PROFILE is unexpected value: {config_space}')
else:
    raise EnvironmentError('CONFIG_PROFILE environment variable is not set!')