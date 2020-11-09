class DataReader:

    @staticmethod
    def read_csv_with_header_and_schema(spark, schema, path):
        return spark.read.option('header', 'true').csv(path, schema=schema)

