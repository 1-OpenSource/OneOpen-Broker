from __future__ import annotations

from oneopen_broker.cli.main import build_parser


def test_cli_parser_commands() -> None:
    parser = build_parser()
    args = parser.parse_args(["stats", "--host", "127.0.0.1", "--port", "6380", "--token", "abc"])
    assert args.command == "stats"
    assert args.token == "abc"
    args = parser.parse_args(["start", "--port", "7000", "--auth-token", "s3cret"])
    assert args.command == "start"
    assert args.port == 7000
    assert args.auth_token == "s3cret"
