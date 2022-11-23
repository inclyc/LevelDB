from cmath import inf
import random
from typing import Tuple
import model
from db_models import *

def generate_write_list(generator: model.generator, timestamp_range: Tuple[int, int], filename: str, n: int, seed: int = None) -> None:
    with open(filename) as f:
        line = generator.generate_write_head()
        f.write(line)
        for i in n:
            l = random.randint(timestamp_range[0], timestamp_range[1])
            r = random.randint(timestamp_range[0], timestamp_range[1])
            l, r = min(l,r), max(l,r)
            line = generator.generate_query(l, r)
            f.write(line)

def generate_query_list(generator: model.generator, timestamp_range: Tuple[int, int], filename: str, seed: int = None) -> None:
    if not seed == None:
        random.seed(seed)
    with open(filename) as f:
        line = generator.generate_query_head()
        f.write(line)
        for timestamp in range(timestamp_range[0], timestamp_range[1]):
            line = generator.generate_query(timestamp, random.randint(0,100))
            f.write(line)
        

if __name__ == '__main__':
    seed = random.randint(0,1000000)
    generator = influxdb.influxdb()
    generate_query_list(generator, (1,2), "influxdb_query", seed)
    generate_query_list(generator, (1,2), "influxdb_data", seed)
    generator = leveldb.leveldb()
    generate_query_list(generator, (1,2), "leveldb_query", seed)
    generate_query_list(generator, (1,2), "leveldb_data", seed)
    generator = mariadb.mariadb()
    generate_query_list(generator, (1,2), "mariadb_query", seed)
    generate_query_list(generator, (1,2), "mariadb_data", seed)