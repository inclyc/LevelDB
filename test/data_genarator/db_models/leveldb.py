import model
class leveldb(model.generator):
    def generate_write(self, timestamp: int, value: int) -> str:
        return f"{timestamp},{value}\n"
    def generate_query(self, left: int, right: int) -> str:
        return f"{left},{right}\n"
    def generate_write_head(self) -> str:
        return "timestamp,data\n"
    def generate_query_head(self) -> str:
        return "l,r\n"