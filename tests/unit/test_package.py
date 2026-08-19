from enterprise_twins import __version__


def test_package_has_release_version() -> None:
    assert __version__ == "0.1.0"
