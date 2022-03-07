import os
import sys
import math
import json
import requests
from datetime import datetime
from Auth.AuthClass import AuthClass

sys.path.append("..")


class DataExtractorClass(object):
    def __init__(self, logger, auth_client: AuthClass, pages_to_scan: int = -1, default: bool = False):
        self.logger = logger
        self._auth_client = auth_client

        if pages_to_scan < 0:
            self._pages_to_scan = math.inf
        elif pages_to_scan == 0:
            raise "At least 1 page to download is required! Aborting..."
        else:
            self._pages_to_scan = pages_to_scan

        if default:
            self._auth_code = "3491024292"
        else:
            self._auth_code = self._auth_client.get_response()

    @property
    def auth_code(self): return self._auth_code

    @property
    def pages_to_scan(self): return self._pages_to_scan

    def extract(self, staging_path: str) -> None:
        current_ts = datetime.now().strftime("%Y-%m-%dT%H%M%S")

        success_filename = f"success_staging_{current_ts}.txt"
        fail_filename = f"fail_staging_{current_ts}.txt"
        success_filepath = os.path.join(staging_path, success_filename)
        fail_filepath = os.path.join(staging_path, fail_filename)

        url = "http://www.alarstudios.com/test/data.cgi"
        headers = {"Accept": "application/json"}

        success_page_index = 0
        fail_page_index = 0
        timeout_exception_counter = 0

        with open(file=success_filepath, mode="w", encoding="utf-8") as success_file, open(file=fail_filepath, mode="w", encoding="utf-8") as fail_file:
            self.logger.info(f"Created staging file {success_filepath} for input data. Starting extraction...")
            while True:
                page_index = success_page_index + fail_page_index
                if page_index == self.pages_to_scan:
                    self.logger.info(f"Downloaded {page_index} page(s)!")
                    break
                payload = {"p": page_index, "code": self.auth_code}
                try:
                    response = requests.get(
                        url=url,
                        headers=headers,
                        params=payload,
                        timeout=100
                    )
                    response.raise_for_status()
                    response_data = response.json()

                    if len(response_data["data"]) == 0:
                        self.logger.info(f"Received page with zero length data field! Number of downloaded pages: {page_index}")
                        break

                    success_file.write(response.text)
                    success_page_index += 1

                except requests.exceptions.HTTPError as err:
                    raise SystemExit(err)
                except requests.exceptions.Timeout:
                    if timeout_exception_counter < 4:
                        self.logger.warning("Request timeout exceeded! Up for retry...")
                        continue
                    else:
                        self.logger.exception("Exceeded timeout limit! Abort...")
                except TypeError:
                    self.logger.exception(f"Unable to serialize the object: {response_data}")
                except json.decoder.JSONDecodeError:
                    self.logger.warning(f"Corrupted page!\nPage index: {page_index}\nBody: {response.text}")
                    fail_file.write(response.text)
                    fail_page_index += 1
                    continue
                except Exception:
                    self.logger.exception("Unexpected exception! Abort...")

        self.logger.info(f"Done!\nSuccess pages: {success_page_index}\nFailed pages: {fail_page_index}")
        self.logger.info("Closing the files for writing")
        return None