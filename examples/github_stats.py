import json

import requests

from dagger import run_tasks, Task


class DownloadGitHub(Task):
    """
    Download statistics about a repository from GitHub API, and store it in a file.
    """

    url_pattern = "https://api.github.com/repos/{}/{}"

    def run(self):
        owner = self.config["owner"]
        repo = self.config["repo"]

        url = self.url_pattern.format(owner, repo)
        response = requests.get(url)

        outfile_name = "{}_{}".format(owner, repo)
        with open(outfile_name, "w") as outfile:
            outfile.write(response.text)


class ComputeStats(Task):
    """
    Count up the total size of all repositories we downloaded.
    """

    def run(self):

        total_size = 0

        for download_task in self.dependencies:

            owner = download_task.config["owner"]
            repo = download_task.config["repo"]
            outfile_name = "{}_{}".format(owner, repo)
            with open(outfile_name) as outfile:
                stats = json.load(outfile)

            total_size += stats["size"]

        print("Total size of all repos:", total_size)


if __name__ == "__main__":

    repos = [("trustyou", "retwist")]

    download_tasks = [DownloadGitHub({"owner": owner, "repo": repo}) for (owner, repo) in repos]
    stats_task = ComputeStats(None, download_tasks)

    run_tasks([stats_task])