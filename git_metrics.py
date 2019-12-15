import copy
import itertools
import logging
import os
import re
import subprocess
import tempfile

from multiprocessing.dummy import Pool as ThreadPool

import yaml


class GitCommandError(Exception):
    pass


class SaasConfigReadError(Exception):
    pass


class GitMetrics(object):
    def __init__(self, repo, **kwargs):
        self.repo = self._canonicalize_url(repo)
        self.bare = kwargs.get('bare', False)
        self.cache = kwargs.get('cache', False)

        if self.cache:
            self.work_dir = os.path.join(self.cache, self._short_repo(repo))
        else:
            self.work_dir = tempfile.mkdtemp(prefix='tmp-git-metrics-',
                                             dir='.')

        if self.bare:
            self.git_dir = self.work_dir
        else:
            self.git_dir = self.work_dir + "/.git"

        self.clone_or_pull()

    def clone_or_pull(self):
        if not os.path.isdir(self.git_dir):
            self.clone()
        else:
            self.pull()

    def clone(self):
        logging.info(['cloning', self.repo])
        if self.bare:
            subprocess.call(['git', 'clone', '--bare', '--quiet', self.repo,
                             self.work_dir])
        else:
            subprocess.call(['git', 'clone', '--quiet', self.repo,
                             self.work_dir])

    def pull(self):
        """
        This methods ensures we are pulling only once per process
        """

        pull_ts_file = os.path.join(self.git_dir, "git_metrics_ts_pull")

        try:
            with open(pull_ts_file, 'r') as f:
                if int(f.read()) == os.getpid():
                    logging.info(['pulling(skipping)', self.repo])
                    return
        except IOError:
            pass

        logging.info(['pulling', self.repo])

        if self.bare:
            self._git_command(["fetch", "-q", "origin", "master"])
            self._git_command(["update-ref", "HEAD", "FETCH_HEAD"])
        else:
            self._git_command(["pull", "-q"])

        with open(pull_ts_file, 'w') as f:
            f.write(str(os.getpid()))

    def get_file(self, commit, path):
        return self._git_command(['show', '{}:{}'.format(commit, path)])

    def ls_files_dir(self, commit, path):
        ls_tree_cmd = ['ls-tree', '--name-only', commit, path.rstrip('/')+'/']
        return self._git_command(ls_tree_cmd).splitlines()

    def count(self, commit="HEAD"):
        try:
            # if the commit is not a valid branch/sha it's probably something
            # like `none`, `ignore`, etc. In those case we want to return total
            # amount of commits
            self.rev_parse(commit)
        except GitCommandError:
            commit = "HEAD"

        fist_command_cmd = ["rev-list", "--max-parents=0", "HEAD"]
        first_commit = self._git_command(fist_command_cmd)

        count_cmd = ['rev-list',
                     '{}..{}'.format(first_commit, commit),
                     '--count']

        return int(self._git_command(count_cmd))

    def commit_ts(self, commit):
        commit_ts_cmd = ['show', '-s', '--format=%ct', commit]
        return int(self._git_command(commit_ts_cmd))

    def rev_parse(self, commit):
        return self._git_command(['rev-parse', '-q', commit])

    def is_sha(self, h):
        return len(str(h)) == 40

    def _git_command(self, cmd):
        devnull = open(os.devnull, 'wb')
        git_cmd = ['git', '--git-dir', self.git_dir] + cmd

        p = subprocess.Popen(git_cmd,
                             stdin=subprocess.PIPE,
                             stderr=devnull,
                             stdout=subprocess.PIPE)

        out, _ = p.communicate()

        if p.returncode != 0:
            msg = "error running {} in {}".format(git_cmd, self.repo)
            raise GitCommandError(msg)

        return out.strip()

    def _short_repo(self, repo):
        gitlab_prefix = 'https://gitlab.cee.redhat.com/'
        github_prefix = 'https://github.com/'

        if repo.startswith(gitlab_prefix):
            provider = 'gitlab'
            short_repo = repo[len(gitlab_prefix):]
        elif repo.startswith(github_prefix):
            short_repo = repo[len(github_prefix):]
            provider = 'github'
        else:
            raise Exception("Unknown provider for {}".format(repo))

        short_repo = self._canonicalize_url(short_repo)
        short_repo_split = short_repo.split("/")

        if len(short_repo_split) != 2:
            raise Exception("Expecting only two components {}".format(repo))

        org, repo = short_repo_split

        # remove '.git'
        suffix = '.git'
        assert repo.endswith(suffix)
        repo = repo[:-len(suffix)]

        return "{}-{}-{}".format(provider, org, repo)

    def _canonicalize_url(self, repo):
        # remove '/' suffix if it exists
        if repo[-1] == '/':
            repo = repo[:-1]

        # add '.git' if necessary
        if not repo.endswith('.git'):
            repo = repo + '.git'

        return repo


class SaasGitMetrics(GitMetrics):
    def __init__(self, repo, poolsize, **kwds):
        super(SaasGitMetrics, self).__init__(repo, **kwds)
        self.poolsize = poolsize
        self.cache = kwds.get('cache', False)
        self.upstream_repo_metrics = {}

        try:
            config_yaml = self.get_file('HEAD', 'config.yaml')
        except GitCommandError:
            raise SaasConfigReadError()

        config = yaml.safe_load(config_yaml)
        self.contexts = config['contexts']

    def get_yaml(self, commit, path):
        data = self._git_command(['show', '{}:{}'.format(commit, path)])
        return yaml.safe_load(data)

    def get_services(self, commit, path):
        try:
            services_file = self.get_yaml(commit, path)
        except (GitCommandError, yaml.scanner.ScannerError):
            return []

        if not isinstance(services_file, dict):
            return []

        services = services_file.get('services', [])
        if not isinstance(services, list):
            return []

        return services

    def fetch_repo_metrics(self, repo):
        repo_metrics = GitMetrics(repo, bare=True, cache=self.cache)
        return (repo, repo_metrics)

    def services(self, commit='HEAD'):
        """
        Returns a dict of tuples {(context, service_name): service}
        """

        return {
            (c['name'], service['name']): service
            for c in self.contexts
            for sf in self.ls_files_dir(commit, c['data']['services_dir'])
            for service in self.get_services(commit, sf)
        }

    def services_with_info(self, commit='HEAD'):
        services = self.services(commit)

        # fetch all repos
        upstream_urls = set([self._canonicalize_url(service['url'])
                             for service in services.values()])

        upstream_urls = list(upstream_urls - set([self.repo]))

        if self.poolsize > 0:
            pool = ThreadPool(self.poolsize)
            repo_metrics_tuples = pool.map(self.fetch_repo_metrics,
                                           upstream_urls)

            repo_metrics = {k: v for (k, v) in repo_metrics_tuples}
        else:
            repo_metrics = {
                repo: self.fetch_repo_metrics(repo)[1]
                for repo in upstream_urls
            }

        repo_metrics[self.repo] = self

        for (context_name, _), service in services.items():
            url = self._canonicalize_url(service['url'])
            h = service['hash']
            if not h:
                h = "HEAD"

            upstream_commits = repo_metrics[url].count()
            upstream_saas_commit_index = repo_metrics[url].count(h)

            service['context'] = context_name
            service['commit'] = self.rev_parse('HEAD')
            service['commit_ts'] = self.commit_ts('HEAD')
            service['upstream_commits'] = upstream_commits
            service['upstream_saas_commit_index'] = upstream_saas_commit_index

        return services

    def services_hash_history(self):
        """
        returns a list of service info objects, where each one
        contains the following:

        {
          "hash": "<hash that has been promoted to prod>",
          "name": "<name of",
          "url": "<url of the upstream repo>",
          "commit_ts": <timestamp of the commit>,
          "upstream_commits": <number of total commits in the upstream repo>,
          "context": "<name of the context>",
          "commit": "<commit that introduced the promote to prod of the hash>",
          "upstream_saas_commit_index": <commit number in upstream of the last
                                         promoted to prod commit>
        }
        """
        services_head = self.services_with_info()

        services_history = {
            service_tuple: {
                k: v
                for k, v in service.items()
                if k in ["hash", "url", "context", "name", "commit",
                         "commit_ts", "upstream_commits",
                         "upstream_saas_commit_index"]
            }
            for service_tuple, service in copy.deepcopy(services_head).items()
        }

        services_found = []
        for index in itertools.count(1):
            if len(services_found) == len(services_head):
                break

            commit = "HEAD~{}".format(index)

            commit_hash = self.rev_parse(commit)
            commit_ts = self.commit_ts(commit)

            services_commit = self.services(commit)

            for service_tuple, service in services_head.items():
                if service_tuple in services_found:
                    continue

                try:
                    commit_hash = services_commit[service_tuple]['hash']
                except KeyError:
                    commit_hash = None

                if not commit_hash or service.get('hash') != commit_hash or \
                        not self.is_sha(service.get('hash')):
                    services_found.append(service_tuple)
                else:
                    services_history[service_tuple]['commit'] = commit_hash
                    services_history[service_tuple]['commit_ts'] = \
                        commit_ts

        return services_history.values()
