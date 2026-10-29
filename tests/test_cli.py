import pytest

from cvm_lake import __version__
from cvm_lake.cli import main


def test_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_no_args_is_ok() -> None:
    assert main([]) == 0
