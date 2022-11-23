import model
class influxdb(model.generator):
    def generate_write(self, timpstamp: int, value: int) -> str:
        pass
    def generate_query(self, timpstamp: int, value: int) -> str:
        pass
    def generate_write_head(self) -> str:
        pass
    def generate_query_head(self) -> str:
        pass