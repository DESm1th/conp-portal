# -*- coding: utf-8 -*-
"""Threading Module

Module that contains the threaded pipeline searching functions
"""
from boutiques.searcher import Searcher
from boutiques.puller import Puller
from pathlib import Path
import configparser
import threading
import json
import os
import logging


def make_logger(name, fname):
    if not os.path.exists("logs"):
        os.makedirs("logs")
    fileh = logging.FileHandler(Path("logs").joinpath(fname))
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    fileh.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(fileh)
    logger.setLevel(logging.INFO)
    return logger

class UpdatePipelineData(threading.Thread):
    """
        Class that handles the threaded updating of the Pipeline
        registrty from Zenodo
    """
    def __init__(self, path):
        super(UpdatePipelineData, self).__init__()
        self.logger = make_logger("UpdatePipelineData", "update_pipeline_data.log")
        self.cache_dir = path

    def run(self):
        try:
            # if cache directory doesn't exist then create it
            if not os.path.exists(self.cache_dir):
                os.makedirs(self.cache_dir)

            # first search for all descriptors
            searcher = Searcher(query="", max_results=100, no_trunc=True)
            all_descriptors = searcher.search()

            # then pull every single descriptor
            all_descriptor_ids = list(map(lambda x: x["ID"], all_descriptors))
            Puller(all_descriptor_ids).pull()

            # fetch every single descriptor into one file
            detailed_all_descriptors = [
                json.load(open(os.path.join(self.cache_dir,
                                            descriptor["ID"].replace(".", "-") + ".json"),
                          "r"))
                for descriptor in all_descriptors
            ]

            # store data in cache
            with open(os.path.join(self.cache_dir,
                                   "all_descriptors.json"),
                      "w") as f:
                json.dump(all_descriptors, f, indent=4)

            with open(os.path.join(self.cache_dir,
                                   "detailed_all_descriptors.json"),
                      "w") as f:
                json.dump(detailed_all_descriptors, f, indent=4)

        except Exception as e:
            self.logger.exception("An exception occurred in the thread.")

class UpdateDatasets(threading.Thread):
    """
    Class that updates the datasets database table
    """
    def __init__(self, path):
        super(UpdateDatasets, self).__init__()
        self.data_path = path
        self.logger = make_logger("UpdateDatasets", "update_datasets.log")

    def run(self):
        for project in self.data_path.glob("*"):
            if not project.is_dir():
                self.logger.info("Non-dataset found in dataset folder: "
                        "{}. Ignoring.".format(project))
                continue

            datalad_conf = project.joinpath(".datalad/config")
            dataset_id = self._get_dataset_id(datalad_conf)


            git_conf = project.joinpath(".git/config")
            annex_uuid = self._get_annex_uuid(git_conf)

    def _get_dataset_id(self, datalad_config):
        try:
            parser = configparser.ConfigParser(strict=False)
            parser.read(datalad_config)
            dataset_id = parser["datalad \"dataset\""]["id"]
        except Exception as e:
            self.logger.error("Failed to read dataset ID. Reason - {}".format(e))
            return None
        return dataset_id

    def _get_annex_uuid(self, git_config):
        try:
            parser = configparser.ConfigParser(strict=False)
            parser.read(git_config)
            annex_uuid = parser["annex"]["uuid"]
        except Exception as e:
            self.logger.error("Failed to read annex UUID. Reason - {}".format(e))
            return None
        return annex_uuid
