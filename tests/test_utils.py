#   Licensed under the Apache License, Version 2.0 (the "License"); you may
#   not use this file except in compliance with the License. You may obtain
#   a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.
#

import pytest
from unittest import mock

from openshift_metrics import utils
import openshift as oc
import openshift.selector
import openshift.apiobject


@pytest.fixture(autouse=True)
def mock_sleep():
    with mock.patch("time.sleep"):
        yield


@mock.patch("requests.get")
def test_query_metric(mock_get):
    mock_response = mock.Mock(status_code=200)
    mock_response.json.return_value = {"data": {"result": "this is data"}}
    mock_get.return_value = mock_response

    metrics = utils.query_metric(
        "fake-url", "fake-token", "fake-metric", "2022-03-14", "2022-03-14"
    )
    assert metrics == "this is data"
    assert mock_get.call_count == 1


@mock.patch("requests.get")
def test_query_metric_exception(mock_get):
    mock_get.return_value = mock.Mock(status_code=404)

    with pytest.raises(Exception):
        utils.query_metric(
            "fake-url", "fake-token", "fake-metric", "2022-03-14", "2022-03-14"
        )
    assert mock_get.call_count == 3


@mock.patch("openshift.selector")
def test_get_namespace_annotations(mock_selector):
    mock_namespaces = mock.Mock()
    mock_namespaces.objects.return_value = [
        oc.apiobject.APIObject(
            {
                "metadata": {
                    "name": "namespace1",
                    "annotations": {"anno1": "value1", "anno2": "value2"},
                }
            }
        ),
        oc.apiobject.APIObject(
            {
                "metadata": {
                    "name": "namespace2",
                    "annotations": {"anno3": "value3", "anno4": "value4"},
                }
            }
        ),
    ]
    mock_selector.return_value = mock_namespaces

    namespaces_dict = utils.get_namespace_annotations()
    expected_namespaces_dict = {
        "namespace1": {"anno1": "value1", "anno2": "value2"},
        "namespace2": {"anno3": "value3", "anno4": "value4"},
    }
    assert expected_namespaces_dict == namespaces_dict


def test_merge_metrics_empty():
    test_metric_list = [
        {
            "metric": {
                "pod": "pod1",
                "namespace": "namespace1",
                "resource": "cpu",
            },
            "values": [
                [0, 10],
                [60, 15],
                [120, 20],
            ],
        },
        {
            "metric": {
                "pod": "pod2",
                "namespace": "namespace1",
                "resource": "cpu",
            },
            "values": [
                [0, 30],
                [60, 35],
                [120, 40],
            ],
        },
    ]
    expected_output_dict = {
        "pod1": {
            "namespace": "namespace1",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu": 10},
                60: {"cpu": 15},
                120: {"cpu": 20},
            },
        },
        "pod2": {
            "namespace": "namespace1",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu": 30},
                60: {"cpu": 35},
                120: {"cpu": 40},
            },
        },
    }
    output_dict = {}
    utils.merge_metrics("cpu", test_metric_list, output_dict)
    assert output_dict == expected_output_dict


def test_merge_metrics_not_empty():
    test_metric_list = [
        {
            "metric": {
                "pod": "pod1",
                "namespace": "namespace1",
            },
            "values": [
                [0, 100],
                [60, 150],
                [120, 200],
            ],
        },
        {
            "metric": {"pod": "pod2", "namespace": "namespace1"},
            "values": [
                [60, 300],
            ],
        },
    ]
    output_dict = {
        "pod1": {
            "namespace": "namespace1",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu": 10},
                60: {"cpu": 15},
                120: {"cpu": 20},
            },
        },
        "pod2": {
            "namespace": "namespace1",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu": 30},
                60: {"cpu": 35},
                120: {"cpu": 40},
            },
        },
    }
    expected_output_dict = {
        "pod1": {
            "namespace": "namespace1",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu": 10, "mem": 100},
                60: {"cpu": 15, "mem": 150},
                120: {"cpu": 20, "mem": 200},
            },
        },
        "pod2": {
            "namespace": "namespace1",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu": 30},
                60: {"cpu": 35, "mem": 300},
                120: {"cpu": 40},
            },
        },
    }
    utils.merge_metrics("mem", test_metric_list, output_dict)
    assert output_dict == expected_output_dict


def test_condense_metrics():
    test_input_dict = {
        "pod1": {
            "metrics": {
                0: {
                    "cpu": 10,
                    "mem": 15,
                },
                60: {
                    "cpu": 10,
                    "mem": 15,
                },
            }
        },
        "pod2": {
            "metrics": {
                0: {
                    "cpu": 2,
                    "mem": 256,
                },
                100: {
                    "cpu": 2,
                    "mem": 256,
                },
            }
        },
    }
    expected_condensed_dict = {
        "pod1": {"metrics": {0: {"cpu": 10, "mem": 15, "duration": 120}}},
        "pod2": {"metrics": {0: {"cpu": 2, "mem": 256, "duration": 200}}},
    }
    condensed_dict = utils.condense_metrics(test_input_dict, ["cpu", "mem"])
    assert condensed_dict == expected_condensed_dict


def test_condense_metrics_no_interval():
    test_input_dict = {
        "pod1": {
            "metrics": {
                0: {
                    "cpu": 10,
                    "mem": 15,
                }
            }
        },
    }
    expected_condensed_dict = {
        "pod1": {"metrics": {0: {"cpu": 10, "mem": 15, "duration": 900}}},
    }
    condensed_dict = utils.condense_metrics(test_input_dict, ["cpu", "mem"])
    assert condensed_dict == expected_condensed_dict


def test_condense_metrics_with_change():
    test_input_dict = {
        "pod2": {
            "metrics": {
                0: {
                    "cpu": 20,
                    "mem": 25,
                },
                60: {
                    "cpu": 20,
                    "mem": 25,
                },
                120: {
                    "cpu": 25,
                    "mem": 25,
                },
                180: {
                    "cpu": 20,
                    "mem": 25,
                },
            }
        },
    }
    expected_condensed_dict = {
        "pod2": {
            "metrics": {
                0: {"cpu": 20, "mem": 25, "duration": 120},
                120: {"cpu": 25, "mem": 25, "duration": 60},
                180: {"cpu": 20, "mem": 25, "duration": 60},
            }
        },
    }
    condensed_dict = utils.condense_metrics(test_input_dict, ["cpu", "mem"])
    assert condensed_dict == expected_condensed_dict


def test_condense_metrics_skip_metric():
    test_input_dict = {
        "pod3": {
            "metrics": {
                0: {
                    "cpu": 30,
                    "mem": 35,
                    "gpu": 1,
                },
                60: {
                    "cpu": 30,
                    "mem": 35,
                    "gpu": 2,
                },
            }
        }
    }
    expected_condensed_dict = {
        "pod3": {"metrics": {0: {"cpu": 30, "mem": 35, "gpu": 1, "duration": 120}}},
    }
    condensed_dict = utils.condense_metrics(test_input_dict, ["cpu", "mem"])
    assert condensed_dict == expected_condensed_dict


@mock.patch("openshift_metrics.utils.get_namespace_annotations")
def test_write_metrics_log_pod(mock_gna, tmp_path):
    mock_gna.return_value = {
        "namespace1": {
            "cf_pi": "PI1",
            "cf_project_id": "123",
        },
        "namespace2": {
            "cf_pi": "PI2",
            "cf_project_id": "456",
        },
    }
    test_metrics_dict = {
        "pod1": {
            "namespace": "namespace1",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu_request": 10, "memory_request": 1048576, "duration": 120},
                120: {"cpu_request": 20, "memory_request": 1048576, "duration": 60},
            },
        },
        "pod2": {
            "namespace": "namespace1",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu_request": 20, "memory_request": 10485760, "duration": 60},
                60: {"cpu_request": 25, "memory_request": 10485760, "duration": 60},
                120: {"cpu_request": 20, "memory_request": 10485760, "duration": 60},
            },
        },
        "pod3": {
            "namespace": "namespace2",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu_request": 45, "memory_request": 104857600, "duration": 180},
            },
        },
        "pod4": {  # this results in 0.5 SU
            "namespace": "namespace2",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu_request": 0.5, "memory_request": 2147483648, "duration": 3600},
            },
        },
    }

    expected_output = (
        "Namespace,Coldfront_PI Name,Coldfront Project ID ,Pod Start Time,Pod End Time,Duration (Hours),Pod Name,CPU Request,GPU Request,GPU Type,Memory Request (GiB),Determining Resource,SU Type,SU Count\n"
        "namespace1,PI1,123,1970-01-01T00:00:00,1970-01-01T00:02:00,0.0333,pod1,10,0,No GPU,0.001,CPU,OpenShift CPU,10.0\n"
        "namespace1,PI1,123,1970-01-01T00:02:00,1970-01-01T00:03:00,0.0167,pod1,20,0,No GPU,0.001,CPU,OpenShift CPU,20.0\n"
        "namespace1,PI1,123,1970-01-01T00:00:00,1970-01-01T00:01:00,0.0167,pod2,20,0,No GPU,0.0098,CPU,OpenShift CPU,20.0\n"
        "namespace1,PI1,123,1970-01-01T00:01:00,1970-01-01T00:02:00,0.0167,pod2,25,0,No GPU,0.0098,CPU,OpenShift CPU,25.0\n"
        "namespace1,PI1,123,1970-01-01T00:02:00,1970-01-01T00:03:00,0.0167,pod2,20,0,No GPU,0.0098,CPU,OpenShift CPU,20.0\n"
        "namespace2,PI2,456,1970-01-01T00:00:00,1970-01-01T00:03:00,0.05,pod3,45,0,No GPU,0.0977,CPU,OpenShift CPU,45.0\n"
        "namespace2,PI2,456,1970-01-01T00:00:00,1970-01-01T01:00:00,1.0,pod4,0.5,0,No GPU,2.0,CPU,OpenShift CPU,0.5\n"
    )

    with tmp_path.joinpath("log.txt").open("w+") as tmp:
        utils.write_metrics_by_pod(test_metrics_dict, tmp.name)
        assert tmp.read() == expected_output


@mock.patch("openshift_metrics.utils.get_namespace_annotations")
def test_write_metrics_log_namespace(mock_gna, tmp_path):
    mock_gna.return_value = {
        "namespace1": {
            "cf_pi": "PI1",
            "cf_project_id": "123",
        },
        "namespace2": {
            "cf_pi": "PI2",
            "cf_project_id": "456",
        },
    }
    test_metrics_dict = {
        "pod1": {
            "namespace": "namespace1",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu_request": 2, "memory_request": 4 * 2**30, "duration": 43200},
                43200: {
                    "cpu_request": 4,
                    "memory_request": 4 * 2**30,
                    "duration": 43200,
                },
            },
        },
        "pod2": {
            "namespace": "namespace1",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {"cpu_request": 4, "memory_request": 1 * 2**30, "duration": 86400},
                86400: {
                    "cpu_request": 20,
                    "memory_request": 1 * 2**30,
                    "duration": 172800,
                },
            },
        },
        "pod3": {
            "namespace": "namespace2",
            "gpu_type": utils.NO_GPU,
            "metrics": {
                0: {
                    "cpu_request": 1,
                    "memory_request": 8 * 2**30,
                    "duration": 172800,
                },
            },
        },
        "pod4": {
            "namespace": "namespace2",
            "gpu_type": utils.GPU_A100,
            "metrics": {
                0: {
                    "cpu_request": 1,
                    "memory_request": 8 * 2**30,
                    "gpu_request": 1,
                    "duration": 172700,  # little under 48 hours, expect to be rounded up in the output
                },
            },
        },
        "pod5": {
            "namespace": "namespace2",
            "gpu_type": utils.GPU_A2,
            "metrics": {
                0: {
                    "cpu_request": 24,
                    "memory_request": 8 * 2**30,
                    "gpu_request": 1,
                    "duration": 172800,
                },
            },
        },
    }

    expected_output = (
        "Invoice Month,Project - Allocation,Project - Allocation ID,Manager (PI),Invoice Email,Invoice Address,Institution,Institution - Specific Code,SU Hours (GBhr or SUhr),SU Type,Rate,Cost\n"
        "2023-01,namespace1,namespace1,PI1,,,,,1128,OpenShift CPU,0.013,14.664\n"
        "2023-01,namespace2,namespace2,PI2,,,,,96,OpenShift CPU,0.013,1.248\n"
        "2023-01,namespace2,namespace2,PI2,,,,,48,OpenShift GPUA100,1.803,86.544\n"
        "2023-01,namespace2,namespace2,PI2,,,,,144,OpenShift GPUA2,0.466,67.104\n"
    )

    with tmp_path.joinpath("log.txt").open("w+") as tmp:
        utils.write_metrics_by_namespace(test_metrics_dict, tmp.name, "2023-01")
        assert tmp.read() == expected_output


# def get_service_unit(cpu_count, memory_count, gpu_count, gpu_type):
@pytest.mark.parametrize(
    "cpu_count, memory_count, gpu_count, gpu_type, exp_su_type, exp_su_count, exp_determining_resource",
    [
        (4, 16, 0, None, utils.SU_CPU, 4, "CPU"),
        (24, 96, 1, utils.GPU_A100, utils.SU_A100_GPU, 1, "GPU"),
        (50, 96, 1, utils.GPU_A100, utils.SU_A100_GPU, 3, "CPU"),
        (24, 100, 1, utils.GPU_A100, utils.SU_A100_GPU, 2, "RAM"),
        (2, 4, 1, utils.GPU_A100, utils.SU_A100_GPU, 1, "GPU"),
        (8, 64, 1, "Unknown_GPU_Type", utils.SU_UNKNOWN_GPU, 1, "GPU"),
        (1, 0, 0, None, utils.SU_UNKNOWN, 0, "CPU"),
        (0, 0, 0, None, utils.SU_UNKNOWN, 0, "CPU"),
        (8, 64, 0, None, utils.SU_CPU, 16, "RAM"),
        (0.5, 0.5, 0, None, utils.SU_CPU, 0.5, "CPU"),
        (0.1, 1, 0, None, utils.SU_CPU, 0.25, "RAM"),
        (0.8, 0.8, 1, utils.GPU_A100, utils.SU_A100_GPU, 1, "GPU"),
    ],
)
def test_service_units(
    cpu_count,
    memory_count,
    gpu_count,
    gpu_type,
    exp_su_type,
    exp_su_count,
    exp_determining_resource,
):
    su_type, su_count, determining_resource = utils.get_service_unit(
        cpu_count, memory_count, gpu_count, gpu_type
    )
    assert su_type == exp_su_type
    assert su_count == exp_su_count
    assert determining_resource == exp_determining_resource
