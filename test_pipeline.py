import uuid

import datajoint as dj
import numpy as np
import pytest

import pipeline


@pytest.fixture
def schema_name():
    return "test_" + uuid.uuid4().hex[:16]


@pytest.fixture()
def schema_models(schema_name):
    schema, models = pipeline.create(schema_name)
    yield schema, models
    schema.drop(force=True)


def test_topological_sort(schema_models):
    schema = schema_models[0]
    tables = dj.Diagram(schema).topological_sort()
    tables = [t.split(".")[1].strip("`") for t in tables]
    tables = [t for t in tables if t.startswith("__")]
    assert tables == [
        "__deconvolve_start",
        "__preprocess_start",
        "__preprocess_done",
        "__preprocess_done__part",
        "__analyze",
        "__deconvolve",
        "__preprocess",
    ]


# Need schema_models to be sure that database was created.
def test_topological_sort_virtual(schema_name, schema_models):
    module = dj.create_virtual_module("schema.py", schema_name)
    schema = getattr(module, "schema")
    tables = dj.Diagram(schema).topological_sort()
    tables = [t.split(".")[1].strip("`") for t in tables]
    tables = [t for t in tables if t.startswith("__")]
    assert tables == [
        "__deconvolve_start",
        "__preprocess_start",
        "__preprocess_done",
        "__preprocess_done__part",
        "__analyze",
        "__deconvolve",
        "__preprocess",
    ]


def test_populate(schema_models):
    m = schema_models[1]
    m.DeconvolveParams.insert1({"deconvolve_params_name": "deconvolve_0"})
    m.PreprocessParams.insert1({"preprocess_params_name": "preprocess_0"})
    m.PreprocessParams.insert1({"preprocess_params_name": "preprocess_1"})
    m.AnalyzeParams.insert1({"analyze_params_name": "analyze_0"})
    m.AnalyzeParams.insert1({"analyze_params_name": "analyze_1"})

    m.ParamsSet.insert(
        [
            {
                "params_set_name": "params_set_000",
                "deconvolve_params_name": "deconvolve_0",
                "preprocess_params_name": "preprocess_0",
                "analyze_params_name": "analyze_0",
            },
            {
                "params_set_name": "params_set_001",
                "deconvolve_params_name": "deconvolve_0",
                "preprocess_params_name": "preprocess_0",
                "analyze_params_name": "analyze_1",
            },
            {
                "params_set_name": "params_set_010",
                "deconvolve_params_name": "deconvolve_0",
                "preprocess_params_name": "preprocess_1",
                "analyze_params_name": "analyze_0",
            },
        ]
    )

    m.Acquisition.insert1({"acquisition_name": "acq_0"})
    m.Image.insert1({"acquisition_name": "acq_0", "rnd": 0})
    m.Image.insert1({"acquisition_name": "acq_0", "rnd": 1})

    m.Acquisition.insert1({"acquisition_name": "acq_1"})
    m.Image.insert1({"acquisition_name": "acq_1", "rnd": 0})
    m.Image.insert1({"acquisition_name": "acq_1", "rnd": 1})
    m.Image.insert1({"acquisition_name": "acq_1", "rnd": 2})

    m.Processing.insert(
        [
            {
                "acquisition_name": "acq_0",
                "params_set_name": "params_set_000",
            },
            {
                "acquisition_name": "acq_0",
                "params_set_name": "params_set_001",
            },
            {
                "acquisition_name": "acq_1",
                "params_set_name": "params_set_010",
            },
        ]
    )

    m.DeconvolveStart.populate()
    deconvolve_start = m.DeconvolveStart.fetch()
    np.testing.assert_equal(
        deconvolve_start,
        np.array(
            [("acq_0", "deconvolve_0"), ("acq_1", "deconvolve_0")],
            dtype=[("acquisition_name", "O"), ("deconvolve_params_name", "O")],
        ),
    )

    m.Deconvolve.populate()

    deconvolve = m.Deconvolve.fetch()
    np.testing.assert_equal(
        deconvolve,
        np.array(
            [
                ("acq_0", 0, "deconvolve_0"),
                ("acq_0", 1, "deconvolve_0"),
                ("acq_1", 0, "deconvolve_0"),
                ("acq_1", 1, "deconvolve_0"),
                ("acq_1", 2, "deconvolve_0"),
            ],
            dtype=[
                ("acquisition_name", "O"),
                ("rnd", "<i8"),
                ("deconvolve_params_name", "O"),
            ],
        ),
    )

    m.PreprocessStart.populate()

    preprocess_start = m.PreprocessStart.fetch()
    np.testing.assert_equal(
        preprocess_start,
        np.array(
            [
                ("acq_0", "deconvolve_0", "preprocess_0"),
                ("acq_1", "deconvolve_0", "preprocess_1"),
            ],
            dtype=[
                ("acquisition_name", "O"),
                ("deconvolve_params_name", "O"),
                ("preprocess_params_name", "O"),
            ],
        ),
    )

    m.Preprocess.populate()

    preprocess = m.Preprocess.fetch()
    preprocess_expected = np.array(
        [
            ("acq_0", 0, "deconvolve_0", "preprocess_0"),
            ("acq_0", 1, "deconvolve_0", "preprocess_0"),
            ("acq_1", 0, "deconvolve_0", "preprocess_1"),
            ("acq_1", 1, "deconvolve_0", "preprocess_1"),
            ("acq_1", 2, "deconvolve_0", "preprocess_1"),
        ],
        dtype=[
            ("acquisition_name", "O"),
            ("rnd", "<i8"),
            ("deconvolve_params_name", "O"),
            ("preprocess_params_name", "O"),
        ],
    )
    np.testing.assert_equal(preprocess, preprocess_expected)

    m.PreprocessDone.populate()

    preprocess_done = m.PreprocessDone.fetch()
    preprocess_done_expected = np.array(
        [
            ("acq_0", "deconvolve_0", "preprocess_0"),
            ("acq_1", "deconvolve_0", "preprocess_1"),
        ],
        dtype=[
            ("acquisition_name", "O"),
            ("deconvolve_params_name", "O"),
            ("preprocess_params_name", "O"),
        ],
    )
    np.testing.assert_equal(preprocess_done, preprocess_done_expected)

    preprocess_done_part = m.PreprocessDone.Part.fetch()
    preprocess_done_part_expected = np.array(
        [
            ("acq_0", "deconvolve_0", "preprocess_0", 0),
            ("acq_0", "deconvolve_0", "preprocess_0", 1),
            ("acq_1", "deconvolve_0", "preprocess_1", 0),
            ("acq_1", "deconvolve_0", "preprocess_1", 1),
            ("acq_1", "deconvolve_0", "preprocess_1", 2),
        ],
        dtype=[
            ("acquisition_name", "O"),
            ("deconvolve_params_name", "O"),
            ("preprocess_params_name", "O"),
            ("rnd", "<i8"),
        ],
    )
    np.testing.assert_equal(preprocess_done_part, preprocess_done_part_expected)

    m.Analyze.populate()

    analyze = m.Analyze.fetch()
    analyze_expected = np.array(
        [
            ("acq_0", "deconvolve_0", "preprocess_0", "analyze_0"),
            ("acq_1", "deconvolve_0", "preprocess_1", "analyze_0"),
            ("acq_0", "deconvolve_0", "preprocess_0", "analyze_1"),
        ],
        dtype=[
            ("acquisition_name", "O"),
            ("deconvolve_params_name", "O"),
            ("preprocess_params_name", "O"),
            ("analyze_params_name", "O"),
        ],
    )
    np.testing.assert_equal(analyze, analyze_expected)
