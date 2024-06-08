from graphframes import GraphFrame

class GraphFramesConnectedComponentsFinder:
    def __init__(self, spark):
        self.spark = spark

    def find_connected_components(self, data):
        vertices = self.spark.createDataFrame(
            [(record_id, record["email"], record["phone"]) for record_id, record in data.items()],
            ["id", "email", "phone"]
        )

        edges = []
        for record_id, record in data.items():
            if record["email"]:
                edges.append((record_id, record["email"]))
            if record["phone"]:
                edges.append((record_id, record["phone"]))

        edges_df = self.spark.createDataFrame(edges, ["src", "dst"])

        g = GraphFrame(vertices, edges_df)

        result = g.connectedComponents().run()
        return result
