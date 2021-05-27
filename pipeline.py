import datajoint as dj


def create(name):
    schema = dj.schema(name)

    class Pipeline:
        class ParamsMixin:
            @property
            def key_source(self):
                return super().key_source & (Pipeline.Processing * Pipeline.ParamsSet)

        @schema
        class Acquisition(dj.Manual):
            definition = """
            acquisition_name: varchar(100)
            """

        @schema
        class Image(dj.Manual):
            definition = """
            -> Acquisition
            rnd: int
            """

        @schema
        class PreprocessParams(dj.Manual):
            definition = """
            preprocess_params_name: varchar(100)
            """

        @schema
        class PreprocessStart(ParamsMixin, dj.Computed):
            definition = """
            -> Acquisition
            -> PreprocessParams
            """

            def make(self, key):
                self.insert1(key)

        @schema
        class Preprocess(dj.Computed):
            definition = """
            -> Image
            -> PreprocessStart
            """

            def make(self, key):
                self.insert1(key)

        @schema
        class PreprocessDone(dj.Computed):
            definition = """
            -> PreprocessStart
            """

            def make(self, key):
                query = Pipeline.Preprocess & key
                num_preprocess = len(query)
                num_image = len(Pipeline.Image & key)
                if num_image == num_preprocess:
                    self.insert1(key)
                    Pipeline.PreprocessDone.Part.insert(
                        dict(key, rnd=rnd) for rnd in query.fetch("rnd")
                    )

            class Part(dj.Part):
                definition = """
                -> PreprocessDone
                -> Preprocess
                """

        @schema
        class AnalyzeParams(dj.Manual):
            definition = """
            analyze_params_name: varchar(100)
            """

        @schema
        class Analyze(ParamsMixin, dj.Computed):
            definition = """
            -> PreprocessDone
            -> AnalyzeParams
            """

            def make(self, key):
                self.insert1(key)

        @schema
        class ParamsSet(dj.Manual):
            definition = """
            params_set_name: varchar(100)
            ---
            -> [nullable] PreprocessParams
            -> [nullable] AnalyzeParams
            """

        @schema
        class Processing(dj.Manual):
            definition = """
            -> Acquisition
            -> ParamsSet
            """

    return schema, Pipeline
