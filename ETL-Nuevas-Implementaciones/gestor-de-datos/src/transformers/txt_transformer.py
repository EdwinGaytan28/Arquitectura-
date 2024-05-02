from datetime import datetime
from src.extractors.txt_extractor import TXTExtractor
from os.path import join
import luigi, os, json

class TXTTransformer(luigi.Task):

    def requires(self):
        return TXTExtractor()

    def run(self):
        result = []
        for file in self.input():
            with file.open() as txt_file:
                next(txt_file)
                for registro in txt_file.read().split(";"):
                    fields = registro.strip().split(",")
                    
                    if len(fields) >= 8:
                        entry = {


                            "description": fields[2],
                            "quantity": fields[3],
                            "price": fields[5],
                            "total": float(fields[3]) * float(fields[5]),
                            "invoice": fields[0],
                            "date": datetime.strptime(fields[4], "%d/%m/%Y %H:%M").strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                            
                            "provider": fields[1],
                            "country": fields[7]
                        }
                        result.append(entry)

        with self.output().open('w') as out:
            out.write(json.dumps(result, indent=4))

    def output(self):
        project_dir = os.path.dirname(os.path.abspath("loader.py"))
        result_dir = join(project_dir, "result")
        return luigi.LocalTarget(join(result_dir, "txt.json"))