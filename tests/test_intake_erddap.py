import intake
import pytest

from intake_erddap import ERDDAPSource


def intake_init():
    # pytest imports this package last, so plugin is not auto-added
    intake.registry["erddap"] = ERDDAPSource


@pytest.mark.skip(reason="Legacy tests")
def test_simple():
    server = "https://cioosatlantic.ca/erddap"
    dataset_id = "SMA_bay_of_exploits"

    d2 = ERDDAPSource(server, dataset_id).read()

    print(len(d2))
    assert len(d2) > 0
