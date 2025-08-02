import csv
from typing import Iterable, Dict

def read_csv_lines(file_name: str) -> Iterable[Dict[str, str]]:
  with open(file_name, 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
      yield dict(row)

csv_file="data/user_part_aa.csv"
csv_iter=read_csv_lines(csv_file)

for fila in csv_iter:
  print(fila)