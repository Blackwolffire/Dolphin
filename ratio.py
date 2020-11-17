class Ratio:
    def __init__(self, data: dict):
        self.id = data['id']
        self.type = data['type']
        self.name = data['name']
        self.is_benchmark_needed = data['is_benchmark_needed']
        self.is_percent = data['is_percent']