

import os
import anyconfig


class Config(dict):

    @classmethod
    def from_env(cls):

        """
        Get a config instance with the default files.
        """

        root = os.environ.get('OSP_CONFIG', '~/.osp')

        # Default paths.
        paths = [
            os.path.join(os.path.dirname(__file__), 'config.yml'),
            os.path.join(root, 'osp.yml'),
        ]

        return cls(paths)

    def __init__(self, paths):

        """
        Initialize the configuration object.

        Args:
            paths (list): YAML paths, from most to least specific.
        """

        config = anyconfig.load(paths, ignore_missing=True)

        return super().__init__(config)
