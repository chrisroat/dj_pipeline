import uuid

import numpy as np
import pytest

import pipeline


@pytest.fixture()
def m():
    schema_name = "test_" + uuid.uuid4().hex[:16]
    schema, models = pipeline.create(schema_name)
    yield models
    schema.drop(force=True)


def test_populate(m):
    m.PreprocessParams.insert1({"preprocess_params_name": "preprocess_0"})
    m.PreprocessParams.insert1({"preprocess_params_name": "preprocess_1"})
    m.AnalyzeParams.insert1({"analyze_params_name": "analyze_0"})
    m.AnalyzeParams.insert1({"analyze_params_name": "analyze_1"})

    m.ParamsSet.insert(
        [
            {
                "params_set_name": "params_set_00",
                "preprocess_params_name": "preprocess_0",
                "analyze_params_name": "analyze_0",
            },
            {
                "params_set_name": "params_set_01",
                "preprocess_params_name": "preprocess_0",
                "analyze_params_name": "analyze_1",
            },
            {
                "params_set_name": "params_set_10",
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
                "params_set_name": "params_set_00",
            },
            {
                "acquisition_name": "acq_0",
                "params_set_name": "params_set_01",
            },
            {
                "acquisition_name": "acq_1",
                "params_set_name": "params_set_10",
            },
        ]
    )

    m.PreprocessStart.populate()

    preprocess_start = m.PreprocessStart.fetch()
    np.testing.assert_equal(
        preprocess_start,
        np.array(
            [("acq_0", "preprocess_0"), ("acq_1", "preprocess_1")],
            dtype=[("acquisition_name", "O"), ("preprocess_params_name", "O")],
        ),
    )

    m.Preprocess.populate()

    preprocess = m.Preprocess.fetch()
    preprocess_expected = np.array(
        [
            ("acq_0", 0, "preprocess_0"),
            ("acq_0", 1, "preprocess_0"),
            ("acq_1", 0, "preprocess_1"),
            ("acq_1", 1, "preprocess_1"),
            ("acq_1", 2, "preprocess_1"),
        ],
        dtype=[
            ("acquisition_name", "O"),
            ("rnd", "<i8"),
            ("preprocess_params_name", "O"),
        ],
    )
    np.testing.assert_equal(preprocess, preprocess_expected)

    m.PreprocessDone.populate()

    preprocess_done = m.PreprocessDone.fetch()
    preprocess_done_expected = np.array(
        [("acq_0", "preprocess_0"), ("acq_1", "preprocess_1")],
        dtype=[("acquisition_name", "O"), ("preprocess_params_name", "O")],
    )
    np.testing.assert_equal(preprocess_done, preprocess_done_expected)

    preprocess_done_part = m.PreprocessDone.Part.fetch()
    preprocess_done_part_expected = np.array(
        [
            ("acq_0", "preprocess_0", 0),
            ("acq_0", "preprocess_0", 1),
            ("acq_1", "preprocess_1", 0),
            ("acq_1", "preprocess_1", 1),
            ("acq_1", "preprocess_1", 2),
        ],
        dtype=[
            ("acquisition_name", "O"),
            ("preprocess_params_name", "O"),
            ("rnd", "<i8"),
        ],
    )
    np.testing.assert_equal(preprocess_done_part, preprocess_done_part_expected)

    m.Analyze.populate()

    analyze = m.Analyze.fetch()
    analyze_expected = np.array(
        [
            ("acq_0", "preprocess_0", "analyze_0"),
            ("acq_1", "preprocess_1", "analyze_0"),
            ("acq_0", "preprocess_0", "analyze_1"),
        ],
        dtype=[
            ("acquisition_name", "O"),
            ("preprocess_params_name", "O"),
            ("analyze_params_name", "O"),
        ],
    )
    np.testing.assert_equal(analyze, analyze_expected)
