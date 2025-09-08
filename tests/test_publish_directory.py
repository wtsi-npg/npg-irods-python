from unittest.mock import MagicMock, patch
from npg_irods.cli import publish_directory
from pytest import LogCaptureFixture


def _main(args: list[str]):
    with patch("sys.argv", ["publish-directory"] + args):
        publish_directory.main()


@patch("npg_irods.cli.publish_directory.publish_directory", autospec=True)
def test_main_normal_case(
    mock_publish_directory: MagicMock, caplog: LogCaptureFixture, capsys
):
    # Arrange
    mock_publish_directory.return_value = (2, 1, 0)

    # Act
    with caplog.at_level("DEBUG", "main"):
        _main(["directory", "/collection"])

    # Assert
    mock_publish_directory.assert_called_once_with(
        "directory",
        "/collection",
        avus=[],
        acl=[],
        filter_fn=None,
        local_checksum=None,
        fill=False,
        force=False,
        handle_exceptions=True,
        num_clients=4,
    )
    assert "Processed all items successfully" in caplog.text
    assert "num_items=2" in caplog.text
    assert "num_processed=1" in caplog.text
    assert "num_errors=0" in caplog.text
