import os
import csv


class ReportGeneratorClass(object):
    def __init__(self, logger, db):
        self.logger = logger
        self.db = db
        self.scripts_path = None
        self.reports_path = None
        self._files = None

    def execute(self, scripts_path, reports_path):
        """
        Handler around files iterator, csv writer and execution method of DB's cursor
        :param scripts_path:
        :param reports_path:
        :return:
        """
        self.scripts_path = scripts_path
        self.reports_path = reports_path
        self._files = [filenames for dirpath, dirnames, filenames in os.walk(self.scripts_path)]
        try:
            for script in self._files[0]:
                report_name = script.split(".")[0] + ".csv"
                script_path = os.path.join(self.scripts_path, script)
                report_path = os.path.join(self.reports_path, report_name)
                self.logger.info(f"Starting {script_path} execution...")
                with open(report_path, "w", encoding="utf-8") as report:
                    writer = csv.writer(report, dialect="excel")
                    self.db.cursor.execute(open(script_path, "r").read())
                    writer.writerow([desc[0] for desc in self.db.cursor.description])
                    for row in self.db.cursor.fetchall():
                        writer.writerow(row)
                    self.logger.info(f"Done! report located in {report_path}")
            self.logger.info("All scripts have been successfully executed!")
        except Exception as e:
            self.logger.exception(f"Got exception on writing reports! {str(e)}")
